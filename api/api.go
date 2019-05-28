package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
	"sort"

	"github.com/etsy/hound/config"
	"github.com/etsy/hound/index"
	"github.com/etsy/hound/searcher"
)

const (
	defaultLinesOfContext uint = 2
	maxLinesOfContext     uint = 20
	defaultFilesOpened    int = 5
)

type Stats struct {
	FilesOpened int
	Duration    int
}

var (
	gSearchers map[string]*searcher.Searcher 
)

func writeJson(w http.ResponseWriter, data interface{}, status int) {
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Panicf("Failed to encode JSON: %v\n", err)
	}
}

func writeResp(w http.ResponseWriter, data interface{}) {
	writeJson(w, data, http.StatusOK)
}

func writeError(w http.ResponseWriter, err error, status int) {
	writeJson(w, map[string]string{
		"Error": err.Error(),
	}, status)
}

type searchResponse struct {
	repo string
	res  *index.SearchResponse
	err  error
}

/**
 * Searches all repos in parallel.
 */
func searchAll(
	query string,
	opts *index.SearchOptions,
	repos []string,
	vrepos []string,
	idx map[string]*searcher.Searcher,
	filesOpened *int,
	duration *int) (map[string]*index.SearchResponse, error) {

	startedAt := time.Now()

	// n: number of repos, an: number of active repo 
	n := len(repos)
	an := 0 

	// use a buffered channel to avoid routine leaks on errs.
	ch := make(chan *searchResponse, n)
	for _, repo := range repos {
		// if repo is not part of searchers, ignore 
		if idx[repo] == nil {
			continue
		}

		an++;
		go func(repo string, vrepos []string) {
			fms, err := idx[repo].Search(query, opts, vrepos)
			ch <- &searchResponse{repo, fms, err}
		}(repo, vrepos)
	}

	res := map[string]*index.SearchResponse{}
	for i := 0; i < an; i++ {
		r := <-ch
		if r.err != nil {
			return nil, r.err
		}

		if r.res.Matches == nil && r.res.VMatches == nil {
			continue
		}

		// check if it's hidden repo
		if len(r.res.VMatches) > 0 {
			for filerepo, vresult := range r.res.VMatches {
				res[filerepo] = &index.SearchResponse{
					Matches: 	vresult,
					FilesWithMatch:	r.res.VFilesWithMatch[filerepo],
 					Revision:	r.res.VRevision[filerepo],
				}
			}
		} else if r.res.Matches != nil {
			res[r.repo] = r.res
		}

		// unset the keys 
		r.res.VMatches = nil
		r.res.VFilesWithMatch = nil
		r.res.VRevision = nil

		*filesOpened += r.res.FilesOpened
	}

	*duration = int(time.Now().Sub(startedAt).Seconds() * 1000)

	return res, nil
}

// Used for parsing flags from form values.
func parseAsBool(v string) bool {
	v = strings.ToLower(v)
	return v == "true" || v == "1" || v == "fosho"
}

func parseAsRepoList(v string, idx map[string]*searcher.Searcher) ([]string,  []string) {
	v = strings.TrimSpace(v)
	var repos []string
	var vrepos []string
	if v == "*" || v == "" {
		for repo := range idx {
			repos = append(repos, repo)
		}
		return repos, vrepos
	}

	// if the repo doesn't exists in idx list, we enable all hidden repos
	useHiddenRepos := false 
	for _, repo := range strings.Split(v, ",") {
		if idx[repo] == nil {
			useHiddenRepos = true
			// stiall add it into vrepos list for later 
			vrepos = append(vrepos, repo)
			continue 
		}
		repos = append(repos, repo)
	}

	// add hidden repo for search 
	if useHiddenRepos == true {
		for repo, searcher := range idx {
			if searcher.IsHidden() == true {
				repos = append(repos, repo)
			}
		}
	}

	// sort here as we need to use sortSearch 
	sort.Strings(vrepos)
	return repos, vrepos
}

func parseAsUintValue(sv string, min, max, def uint) uint {
	iv, err := strconv.ParseUint(sv, 10, 54)
	if err != nil {
		return def
	}
	if max != 0 && uint(iv) > max {
		return max
	}
	if min != 0 && uint(iv) < min {
		return max
	}
	return uint(iv)
}

func parseRangeInt(v string, i *int) {
	*i = 0
	if v == "" {
		return
	}

	vi, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return
	}

	*i = int(vi)
}

func parseRangeValue(rv string) (int, int) {
	ix := strings.Index(rv, ":")
	if ix < 0 {
		return 0, 0
	}

	var b, e int
	parseRangeInt(rv[:ix], &b)
	parseRangeInt(rv[ix+1:], &e)
	return b, e
}

func SetSearchers(searchers map[string]*searcher.Searcher) {
	// record it as global searchers when setup. it will be updated during hot-reloading 
	gSearchers = searchers
}

func GetSearchers() map[string]*searcher.Searcher {
	return gSearchers
}

func checkReady(w http.ResponseWriter) bool {
	if gSearchers == nil || len(gSearchers) <= 0 {
		writeError(w, errors.New("Server is not ready, please wait..."), http.StatusOK)
		return false
	}

	return true 
}

func Setup(m *http.ServeMux) {

	m.HandleFunc("/api/v1/repos", func(w http.ResponseWriter, r *http.Request) {
		if checkReady(w) == false {
			return
		}

		res := map[string]*config.Repo{}
		for name, searcher := range gSearchers {
			if searcher.IsHidden() == true {
				vrepos := searcher.GetVRepos()
				for _, v := range vrepos {
					res[v] = &config.Repo {
						UrlPattern: searcher.Repo.UrlPattern,
						Revision: searcher.GetVRepoRev(v),
					}
				}
			} else {
				res[name] = searcher.Repo
			}
		}

		writeResp(w, res)
	})

	m.HandleFunc("/api/v1/search", func(w http.ResponseWriter, r *http.Request) {
		if checkReady(w) == false {
			return
		}

		var opt index.SearchOptions

		stats := parseAsBool(r.FormValue("stats"))
		repos, vrepos := parseAsRepoList(r.FormValue("repos"), gSearchers)
		query := r.FormValue("q")
		opt.Offset, opt.Limit = parseRangeValue(r.FormValue("rng"))
		opt.FileRegexp = r.FormValue("files")
		opt.IgnoreCase = parseAsBool(r.FormValue("i"))
		opt.LinesOfContext = parseAsUintValue(
			r.FormValue("ctx"),
			0,
			maxLinesOfContext,
			defaultLinesOfContext)

		// opt.Limit must not be too large if repo is more than one 
		if len(repos) > 1 {
			opt.Limit = defaultFilesOpened
		}

		query = strings.TrimSpace(query)
		if len(query) <= 0 {
			writeError(w, errors.New("No query"), http.StatusOK)
			return
		}

		var filesOpened int
		var durationMs int

		results, err := searchAll(query, &opt, repos, vrepos, gSearchers, &filesOpened, &durationMs)
		if err != nil {
			// TODO(knorton): Return ok status because the UI expects it for now.
			writeError(w, err, http.StatusOK)
			return
		}

		var res struct {
			Results map[string]*index.SearchResponse
			Stats   *Stats `json:",omitempty"`
		}

		res.Results = results
		if stats {
			res.Stats = &Stats{
				FilesOpened: filesOpened,
				Duration:    durationMs,
			}
		}

		writeResp(w, &res)
	})

	m.HandleFunc("/api/v1/excludes", func(w http.ResponseWriter, r *http.Request) {
		if checkReady(w) == false {
			return
		}

		repo := r.FormValue("repo")
		res := "[]"

		if gSearchers[repo] == nil {
			for _, searcher := range gSearchers {
				if searcher.IsHidden() == true {
					vrepos := searcher.GetVRepos()
					i := sort.SearchStrings(vrepos, repo)
					if i < len(vrepos) && vrepos[i] == repo {
						res = searcher.GetExcludedFiles(repo)
						break
					}
				}
			}
		} else {
			res = gSearchers[repo].GetExcludedFiles("")
		}

		w.Header().Set("Content-Type", "application/json;charset=utf-8")
		w.Header().Set("Access-Control-Allow", "*")
		fmt.Fprint(w, res)
	})

	m.HandleFunc("/api/v1/update", func(w http.ResponseWriter, r *http.Request) {
		if checkReady(w) == false {
			return
		}

		if r.Method != "POST" {
			writeError(w,
				errors.New(http.StatusText(http.StatusMethodNotAllowed)),
				http.StatusMethodNotAllowed)
			return
		}

		repos, _ := parseAsRepoList(r.FormValue("repos"), gSearchers)

		for _, repo := range repos {
			searcher := gSearchers[repo]
			if searcher == nil {
				writeError(w,
					fmt.Errorf("No such repository: %s", repo),
					http.StatusNotFound)
				return
			}

			if !searcher.Update() {
				writeError(w,
					fmt.Errorf("Push updates are not enabled for repository %s", repo),
					http.StatusForbidden)
				return

			}
		}

		writeResp(w, "ok")
	})
}
