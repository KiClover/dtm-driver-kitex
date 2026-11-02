package compat

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/dtm-labs/dtmdriver"
	"github.com/dtm-labs/dtmdriver-kitex"
	"google.golang.org/grpc/resolver"
)

func TestDriverAvailableThroughDTM(t *testing.T) {
	if err := dtmdriver.Use(dtmkitex.DriverName); err != nil {
		t.Fatalf("dtmdriver.Use(%q): %v", dtmkitex.DriverName, err)
	}
	var driver dtmdriver.Driver = dtmdriver.GetDriver()
	if driver == nil || driver.GetName() != dtmkitex.DriverName {
		t.Fatalf("registered driver = %#v", driver)
	}
	for _, scheme := range []string{dtmkitex.SchemeEtcd, dtmkitex.SchemeConsul, dtmkitex.SchemeNacos} {
		if resolver.Get(scheme) == nil {
			t.Errorf("grpc resolver %q is not registered", scheme)
		}
	}
}

func TestPublicAPISurface(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate compat test")
	}
	root := filepath.Dir(filepath.Dir(filename))
	pkgs, err := parser.ParseDir(token.NewFileSet(), root, func(info fs.FileInfo) bool {
		return !strings.HasSuffix(info.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{
		"DriverName": true, "SchemeEtcd": true, "SchemeConsul": true, "SchemeNacos": true,
		"ServerMetadataMiddleware": true,
	}
	var exported []string
	for _, decl := range pkgs["dtmkitex"].Files {
		for _, d := range decl.Decls {
			switch d := d.(type) {
			case *ast.FuncDecl:
				if d.Recv == nil && d.Name.IsExported() {
					exported = append(exported, d.Name.Name)
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					switch spec := spec.(type) {
					case *ast.TypeSpec:
						if spec.Name.IsExported() {
							exported = append(exported, spec.Name.Name)
						}
					case *ast.ValueSpec:
						for _, name := range spec.Names {
							if name.IsExported() {
								exported = append(exported, name.Name)
							}
						}
					}
				}
			}
		}
	}
	sort.Strings(exported)
	seen := make(map[string]bool, len(exported))
	for _, name := range exported {
		seen[name] = true
		if !allowed[name] {
			t.Errorf("unexpected exported API %s", name)
		}
	}
	for name := range allowed {
		if !seen[name] {
			t.Errorf("missing exported API %s", name)
		}
	}
}
