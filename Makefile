
ALL: ui/bindata.go

.build/bin/go-bindata:
	GOPATH=`pwd`/.build go get github.com/jteeuwen/go-bindata/...

ui/bindata.go: .build/bin/go-bindata $(wildcard ui/assets/**/*)
	rsync -r --exclude '*.js' ui/assets/* .build/ui
	ui/node_modules/.bin/babel --out-dir=.build/ui/js ui/assets/js
	cp ui/assets/js/*.min.js .build/ui/js/
	$< -o $@ -pkg ui -prefix .build/ui -nomemcopy .build/ui/...

clean:
	rm -rf .build
