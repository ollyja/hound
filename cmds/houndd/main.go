package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"path/filepath"
	"time"
	"errors"

	"github.com/etsy/hound/api"
	"github.com/etsy/hound/config"
	"github.com/etsy/hound/searcher"
	"github.com/etsy/hound/ui"
)

const gracefulShutdownSignal = syscall.SIGTERM

type scanCallback func(path string)

var (
	info_log  *log.Logger
	error_log *log.Logger
	startTime = time.Now()
)

func makeAllSearchers(cfg *config.Config) (map[string]*searcher.Searcher, bool, error) {
	// Ensure we have a dbpath
	if _, err := os.Stat(cfg.DbPath); err != nil {
		if err := os.MkdirAll(cfg.DbPath, os.ModePerm); err != nil {
			return nil, false, err
		}
	}

	searchers, errs, err := searcher.MakeAll(cfg)
	if err != nil {
		return nil, false, err
	}

	if len(errs) > 0 {
		// NOTE: This mutates the original config so the repos
		// are not even seen by other code paths.
		for name, _ := range errs {
			delete(cfg.Repos, name)
		}

		return searchers, false, nil
	}

	return searchers, true, nil
}

func makeSearchers(cfg *config.Config) (map[string]*searcher.Searcher, bool, error) {
	// Ensure we have a dbpath
	if _, err := os.Stat(cfg.DbPath); err != nil {
		if err := os.MkdirAll(cfg.DbPath, os.ModePerm); err != nil {
			return nil, false, err
		}
	}

	searchers, errs, err := searcher.Make(cfg)
	if err != nil {
		return nil, false, err
	}

	if len(errs) > 0 {
		return searchers, false, nil
	}

	return searchers, true, nil
}

func handleShutdown(shutdownCh <-chan os.Signal) {
	go func() {
		<-shutdownCh
		info_log.Printf("Graceful shutdown requested...")
		for _, s := range api.GetSearchers() {
			s.Stop()
		}

		for _, s := range api.GetSearchers() {
			s.Wait()
		}

		os.Exit(0)
	}()
}

func registerShutdownSignal() <-chan os.Signal {
	shutdownCh := make(chan os.Signal, 1)
	signal.Notify(shutdownCh, gracefulShutdownSignal)
	return shutdownCh
}

func makeTemplateData(cfg *config.Config) (interface{}, error) {
	var data struct {
		ReposAsJson string
	}

	res := map[string]*config.Repo{}
	for name, repo := range cfg.Repos {
		res[name] = repo
	}

	b, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}

	data.ReposAsJson = string(b)
	return &data, nil
}

func runHttp(
	addr string,
	dev bool,
	cfg *config.Config,
	idx map[string]*searcher.Searcher) error {
	m := http.DefaultServeMux

	h, err := ui.Content(dev, cfg)
	if err != nil {
		return err
	}

	m.Handle("/", h)
	api.Setup(m, idx)
	return http.ListenAndServe(addr, m)
}

func scanChanges(
	watchPath string, 
	allFiles bool, cb scanCallback) {
	for {
		filepath.Walk(watchPath, func(path string, info os.FileInfo, err error) error {
			if path == ".git" && info.IsDir() {
				return filepath.SkipDir
			}
			/*
			for _, x := range excludeDirs {
				if x == path {
					return filepath.SkipDir
				}
			}
			*/
			// ignore hidden files
			if filepath.Base(path)[0] == '.' {
				return nil
			}

			if (allFiles || filepath.Ext(path) == ".go") && info.ModTime().After(startTime) {
				cb(path)
				startTime = time.Now()
				return errors.New("done")
			}

			return nil
		})
		time.Sleep(500 * time.Millisecond)
	}
}

func checkConfigChange(
	filename string,
	cfg *config.Config) {
	// scan for changes
	go func() {
		scanChanges(filename, true, func(path string) {
			var cfgn config.Config
			if err := cfgn.LoadFromFile(path); err != nil {
				panic(err)
				os.Exit(0)
			}

			deleted := map[string]string{}
			// remove not changed repo 
			for name, repo := range cfg.Repos {
				repo1, ok := cfgn.Repos[name]
				if ok && repo.ToJsonString() == repo1.ToJsonString() {
					info_log.Println("no change for: ", name)
					// no change 
					delete(cfgn.Repos, name)
				} else if ok {
				info_log.Println("config json: ",  repo.ToJsonString())
				info_log.Println("config json: ",  repo1.ToJsonString())
					// the config is udpated, need to restart 
					info_log.Println("config is altered, will restart: ", name)
					deleted[name] = name
				} else {
					// not found. this was removed from config file 
					// need to stop it 
					info_log.Println("deleted, remove from cfg: ", name)
					delete(cfg.Repos,  name)
					deleted[name] = name
				}
			}

			// add new config back into cfg.Repos for next loop
			for name, repo := range cfgn.Repos {
				_, ok := cfg.Repos[name];
				if !ok {
					cfg.Repos[name] = repo
				} else if _, ok = deleted[name]; ok {
					// in cfg.Repos but also in deleted for restart, then its config 
					// has been updated, so add it cfg.Repos for next loop 
					cfg.Repos[name] = repo
				}
			}


			// getCurrent searchers which is a reference to api gSearchers object 
			searchers := api.GetSearchers()
			// disable deleted repos
			if len(deleted) > 0 {
				for name, s := range searchers {
					if  _, ok :=  deleted[name]; ok {
						info_log.Println("searcher stopped: " , name)
						s.Stop()
						s.Wait()
						delete(searchers, name)
					}
				}
			}

			// create new searchers with new config 
			idxn, ok, err := makeSearchers(&cfgn)
			if err != nil {
				log.Panic(err)
			}
			if !ok {
				info_log.Println("Some repos failed to index, see output above")
			} else {
				info_log.Println("All indexes are rebuilt!")
			}

			// add back to global searchers 
			for name, s := range idxn {
				searchers[name] = s
			}

		})
	}()
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	info_log = log.New(os.Stdout, "", log.LstdFlags)
	error_log = log.New(os.Stderr, "", log.LstdFlags)

	flagConf := flag.String("conf", "config.json", "")
	flagAddr := flag.String("addr", ":6080", "")
	flagDev := flag.Bool("dev", false, "")

	flag.Parse()

	var cfg config.Config
	if err := cfg.LoadFromFile(*flagConf); err != nil {
		panic(err)
	}

	// It's not safe to be killed during makeAllSearchers, so register the
	// shutdown signal here and defer processing it until we are ready.
	shutdownCh := registerShutdownSignal()
	idx, ok, err := makeAllSearchers(&cfg)
	if err != nil {
		log.Panic(err)
	}
	if !ok {
		info_log.Println("Some repos failed to index, see output above")
	} else {
		info_log.Println("All indexes built!")
	}

	// enable hot-reload
	checkConfigChange(*flagConf, &cfg)

	// handle graceful shutdown 
	handleShutdown(shutdownCh)

	host := *flagAddr
	if strings.HasPrefix(host, ":") {
		host = "localhost" + host
	}

	info_log.Printf("running server at http://%s...\n", host)

	if err := runHttp(*flagAddr, *flagDev, &cfg, idx); err != nil {
		panic(err)
	}
}
