package vcs

import (
	"fmt"
	"os"
	"strings"
	"path/filepath"

	"github.com/etsy/hound/config"
)

func init() {
	Register(newLocal, "local")
}

type LocalDriver struct{}

func newLocal(b []byte) (Driver, error) {
	return &LocalDriver{}, nil
}

func (g *LocalDriver) WorkingDirForRepo(dbpath string, repo *config.Repo) (string, error) {
	return strings.TrimPrefix(repo.Url, "file://"), nil
}

func (g *LocalDriver) HeadRev(dir string) (string, error) {
	realdir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		fmt.Printf("Failed to read symlink ", dir)
		return "", err
	}

	stat, err := os.Stat(realdir)
	if err != nil {
		fmt.Println("failed to determine modification time of ", realdir)
		return "", err
	}

	return stat.ModTime().String(), nil
}

func (g *LocalDriver) Pull(dir string) (string, error) {
	return g.HeadRev(dir)
}

func (g *LocalDriver) Clone(dir, url string) (string, error) {
	// For local driver Clone() is only called when the directory
	// pointed by url is not found.
	err := fmt.Errorf("Location %s not found.", url)
	fmt.Print(err)
	return "", err
}

func (g *LocalDriver) SpecialFiles() []string {
	return []string{
		".bzr",
		".git",
		".hg",
		".svn",
	}
}
