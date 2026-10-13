// The Go AST analyzer embedded by code_scan.rs. It compiles only this standard
// library program; application source is parsed as data, never built or run.
package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

type request struct {
	Root   string  `json:"root"`
	File   string  `json:"file"`
	Source *string `json:"source"`
}

type callSite struct {
	File           string  `json:"file"`
	Line           int     `json:"line"`
	Entity         string  `json:"entity"`
	Method         string  `json:"method"`
	Action         string  `json:"action"`
	Purpose        *string `json:"purpose"`
	PurposeDynamic bool    `json:"purpose_dynamic"`
}

type analyzer struct {
	positions *token.FileSet
	// Retain every possible assignment to a lexical object. Conditional
	// reassignment cannot silently hide a store call under another entity.
	assignments map[*ast.Object][]ast.Expr
	txContexts  map[*ast.CallExpr]ast.Expr
}

func action(method string) string {
	switch method {
	case "Get", "Select", "SelectAll", "Count", "First":
		return "Select"
	case "Upsert", "Insert":
		return "Upsert"
	case "Patch", "UpdateIf", "UpdateWithRetry", "Increment":
		return "Update"
	case "Delete":
		return "Delete"
	case "Emit":
		return "EnqueueOutboxEvent"
	default:
		return ""
	}
}

func typeName(expression ast.Expr) string {
	switch node := expression.(type) {
	case *ast.Ident:
		return node.Name
	case *ast.SelectorExpr:
		return node.Sel.Name
	case *ast.StarExpr:
		return typeName(node.X)
	case *ast.ParenExpr:
		return typeName(node.X)
	case *ast.IndexExpr:
		return typeName(node.X)
	case *ast.IndexListExpr:
		return typeName(node.X)
	}
	return ""
}

func indexedType(expression ast.Expr, name string) ast.Expr {
	switch node := expression.(type) {
	case *ast.IndexExpr:
		if typeName(node.X) == name {
			return node.Index
		}
	case *ast.IndexListExpr:
		if typeName(node.X) == name && len(node.Indices) == 1 {
			return node.Indices[0]
		}
	case *ast.StarExpr:
		return indexedType(node.X, name)
	case *ast.ParenExpr:
		return indexedType(node.X, name)
	}
	return nil
}

func entityType(expression ast.Expr) string {
	return resolveEntityType(expression, map[*ast.Object]bool{})
}

func resolveEntityType(expression ast.Expr, visiting map[*ast.Object]bool) string {
	if argument := indexedType(expression, "Table"); argument != nil {
		return typeName(argument)
	}
	if name, ok := expression.(*ast.Ident); ok && name.Obj != nil && !visiting[name.Obj] {
		visiting[name.Obj] = true
		defer delete(visiting, name.Obj)
		if declaration, ok := name.Obj.Decl.(*ast.TypeSpec); ok {
			return resolveEntityType(declaration.Type, visiting)
		}
	}
	if pointer, ok := expression.(*ast.StarExpr); ok {
		return resolveEntityType(pointer.X, visiting)
	}
	return ""
}

func namedValue(names []*ast.Ident, values []ast.Expr, object *ast.Object) ast.Expr {
	for index, name := range names {
		if name.Obj == object && index < len(values) {
			return values[index]
		}
	}
	return nil
}

func assignmentValue(statement *ast.AssignStmt, object *ast.Object) ast.Expr {
	for index, expression := range statement.Lhs {
		if name, ok := expression.(*ast.Ident); ok && name.Obj == object && index < len(statement.Rhs) {
			return statement.Rhs[index]
		}
	}
	return nil
}

func objectType(object *ast.Object) ast.Expr {
	return resolveObjectType(object, map[*ast.Object]bool{})
}

func resolveObjectType(object *ast.Object, visiting map[*ast.Object]bool) ast.Expr {
	if object == nil || visiting[object] {
		return nil
	}
	visiting[object] = true
	defer delete(visiting, object)
	switch declaration := object.Decl.(type) {
	case *ast.Field:
		return declaration.Type
	case *ast.ValueSpec:
		if declaration.Type != nil {
			return declaration.Type
		}
		return resolveValueType(namedValue(declaration.Names, declaration.Values, object), visiting)
	case *ast.AssignStmt:
		return resolveValueType(assignmentValue(declaration, object), visiting)
	case *ast.TypeSpec:
		return declaration.Type
	}
	return nil
}

func valueType(expression ast.Expr) ast.Expr {
	return resolveValueType(expression, map[*ast.Object]bool{})
}

func resolveValueType(expression ast.Expr, visiting map[*ast.Object]bool) ast.Expr {
	switch node := expression.(type) {
	case *ast.CompositeLit:
		return node.Type
	case *ast.UnaryExpr:
		return resolveValueType(node.X, visiting)
	case *ast.ParenExpr:
		return resolveValueType(node.X, visiting)
	case *ast.Ident:
		return resolveObjectType(node.Obj, visiting)
	}
	return nil
}

func structType(expression ast.Expr, visiting map[*ast.Object]bool) *ast.StructType {
	switch node := expression.(type) {
	case *ast.StructType:
		return node
	case *ast.StarExpr:
		return structType(node.X, visiting)
	case *ast.ParenExpr:
		return structType(node.X, visiting)
	case *ast.IndexExpr:
		return structType(node.X, visiting)
	case *ast.IndexListExpr:
		return structType(node.X, visiting)
	case *ast.Ident:
		if node.Obj != nil && !visiting[node.Obj] {
			visiting[node.Obj] = true
			defer delete(visiting, node.Obj)
			return structType(objectType(node.Obj), visiting)
		}
	}
	return nil
}

func (a *analyzer) stores(expression ast.Expr, visiting map[*ast.Object]bool) []string {
	if expression == nil {
		return nil
	}
	switch node := expression.(type) {
	case *ast.CallExpr:
		if argument := indexedType(node.Fun, "TableOf"); argument != nil {
			if entity := typeName(argument); entity != "" {
				return []string{entity}
			}
		}
		name := typeName(node.Fun)
		first, _ := utf8.DecodeRuneInString(name)
		if unicode.IsUpper(first) && strings.HasSuffix(name, "Table") && len(name) > len("Table") {
			return []string{strings.TrimSuffix(name, "Table")}
		}
		if function, ok := node.Fun.(*ast.Ident); ok && function.Obj != nil {
			if declaration, ok := function.Obj.Decl.(*ast.FuncDecl); ok && declaration.Type.Results != nil {
				for _, field := range declaration.Type.Results.List {
					if entity := entityType(field.Type); entity != "" {
						return []string{entity}
					}
				}
			}
		}
	case *ast.Ident:
		object := node.Obj
		if object == nil || visiting[object] {
			return nil
		}
		visiting[object] = true
		defer delete(visiting, object)
		var entities []string
		if entity := entityType(objectType(object)); entity != "" {
			entities = append(entities, entity)
		}
		if declaration, ok := object.Decl.(*ast.ValueSpec); ok {
			entities = append(entities, a.stores(namedValue(declaration.Names, declaration.Values, object), visiting)...)
		}
		for _, value := range a.assignments[object] {
			entities = append(entities, a.stores(value, visiting)...)
		}
		return entities
	case *ast.SelectorExpr:
		if structure := structType(valueType(node.X), map[*ast.Object]bool{}); structure != nil {
			for _, field := range structure.Fields.List {
				for _, name := range field.Names {
					if name.Name == node.Sel.Name {
						if entity := entityType(field.Type); entity != "" {
							return []string{entity}
						}
					}
				}
			}
		}
	case *ast.ParenExpr:
		return a.stores(node.X, visiting)
	}
	return nil
}

func (a *analyzer) bind(file *ast.File) {
	ast.Inspect(file, func(node ast.Node) bool {
		if assignment, ok := node.(*ast.AssignStmt); ok {
			for _, expression := range assignment.Lhs {
				if name, ok := expression.(*ast.Ident); ok && name.Obj != nil {
					if value := assignmentValue(assignment, name.Obj); value != nil {
						a.assignments[name.Obj] = append(a.assignments[name.Obj], value)
					}
				}
			}
		}
		return true
	})
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || len(call.Args) != 2 {
			return true
		}
		method, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || method.Sel.Name != "Tx" {
			return true
		}
		body, ok := call.Args[1].(*ast.FuncLit)
		if !ok {
			return true
		}
		ast.Inspect(body.Body, func(child ast.Node) bool {
			if operation, ok := child.(*ast.CallExpr); ok {
				a.txContexts[operation] = call.Args[0]
			}
			return true
		})
		return true
	})
}

func (a *analyzer) methods(expression ast.Expr, visiting map[*ast.Object]bool) []*ast.SelectorExpr {
	switch node := expression.(type) {
	case *ast.SelectorExpr:
		if action(node.Sel.Name) != "" {
			return []*ast.SelectorExpr{node}
		}
	case *ast.ParenExpr:
		return a.methods(node.X, visiting)
	case *ast.Ident:
		object := node.Obj
		if object == nil || visiting[object] {
			return nil
		}
		visiting[object] = true
		defer delete(visiting, object)
		var methods []*ast.SelectorExpr
		if declaration, ok := object.Decl.(*ast.ValueSpec); ok {
			methods = append(methods, a.methods(namedValue(declaration.Names, declaration.Values, object), visiting)...)
		}
		for _, value := range a.assignments[object] {
			methods = append(methods, a.methods(value, visiting)...)
		}
		return methods
	}
	return nil
}

func (a *analyzer) scan(file *ast.File) []callSite {
	sites := []callSite{}
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		var purpose *string
		var purposeDynamic bool
		if len(call.Args) > 0 {
			purpose, purposeDynamic = a.purpose(call.Args[0], false, map[*ast.Object]bool{})
		}
		seen := map[string]bool{}
		for _, method := range a.methods(call.Fun, map[*ast.Object]bool{}) {
			position := a.positions.Position(call.Fun.Pos())
			if direct, ok := call.Fun.(*ast.SelectorExpr); ok {
				position = a.positions.Position(direct.Sel.Pos())
			}
			entities := a.stores(method.X, map[*ast.Object]bool{})
			if typeName(valueType(method.X)) == "TxScope" {
				entity := "<unresolved>"
				if len(call.Args) > 0 {
					if resolved := typeName(valueType(call.Args[0])); resolved != "" {
						entity = resolved
					}
				}
				entities = []string{entity}
				purpose, purposeDynamic = nil, false
				if context := a.txContexts[call]; context != nil {
					purpose, purposeDynamic = a.purpose(context, false, map[*ast.Object]bool{})
				}
			}
			for _, entity := range entities {
				key := entity + "\x00" + method.Sel.Name
				if entity != "" && !seen[key] {
					seen[key] = true
					sites = append(sites, callSite{
						File: position.Filename, Line: position.Line, Entity: entity,
						Method: method.Sel.Name, Action: action(method.Sel.Name),
						Purpose: purpose, PurposeDynamic: purposeDynamic,
					})
				}
			}
		}
		return true
	})
	return sites
}

// Resolve an explicit WithMetadata request purpose. Ordinary context parameters
// inherit the deployment purpose passed to the CLI. An explicit metadata value
// that cannot be resolved, or branch assignments that disagree, is reported as
// dynamic rather than quietly checked against a guessed purpose.
func (a *analyzer) purpose(expression ast.Expr, metadata bool, visiting map[*ast.Object]bool) (*string, bool) {
	switch node := expression.(type) {
	case *ast.ParenExpr:
		return a.purpose(node.X, metadata, visiting)
	case *ast.UnaryExpr:
		return a.purpose(node.X, metadata, visiting)
	case *ast.CallExpr:
		if !metadata && typeName(node.Fun) == "WithMetadata" && len(node.Args) == 2 {
			return a.purpose(node.Args[1], true, visiting)
		}
	case *ast.CompositeLit:
		if metadata {
			for _, element := range node.Elts {
				if field, ok := element.(*ast.KeyValueExpr); ok && typeName(field.Key) == "Purpose" {
					literal, ok := field.Value.(*ast.BasicLit)
					if !ok || literal.Kind != token.STRING {
						return nil, true
					}
					value, err := strconv.Unquote(literal.Value)
					if err != nil {
						return nil, true
					}
					if value == "" {
						return nil, false
					}
					return &value, false
				}
			}
			return nil, false
		}
	case *ast.Ident:
		object := node.Obj
		if object == nil {
			return nil, metadata
		}
		if visiting[object] {
			return nil, true
		}
		visiting[object] = true
		defer delete(visiting, object)
		values := a.assignments[object]
		if declaration, ok := object.Decl.(*ast.ValueSpec); ok {
			if value := namedValue(declaration.Names, declaration.Values, object); value != nil {
				values = append(append([]ast.Expr{}, values...), value)
			}
		}
		if len(values) == 0 {
			return nil, metadata
		}
		var result *string
		for index, value := range values {
			resolved, dynamic := a.purpose(value, metadata, visiting)
			if dynamic {
				return nil, true
			}
			if index == 0 {
				result = resolved
				continue
			}
			if (result == nil) != (resolved == nil) || (result != nil && *result != *resolved) {
				return nil, true
			}
		}
		return result, false
	}
	return nil, metadata
}

func analyze(input request) ([]callSite, error) {
	a := analyzer{positions: token.NewFileSet(), assignments: map[*ast.Object][]ast.Expr{}, txContexts: map[*ast.CallExpr]ast.Expr{}}
	sites := []callSite{}
	packages := map[string][]*ast.File{}
	parse := func(filename string, source any) error {
		// Object resolution supplies lexical bindings for local aliases and
		// shadowing. Syntax errors are fatal; never scan a partial AST.
		file, err := parser.ParseFile(a.positions, filename, source, parser.AllErrors)
		if err != nil {
			return err
		}
		key := filepath.Dir(filename) + "\x00" + file.Name.Name
		packages[key] = append(packages[key], file)
		return nil
	}
	if input.Source != nil {
		if err := parse(input.File, *input.Source); err != nil {
			return nil, err
		}
	} else {
		if input.Root == "" {
			return nil, fmt.Errorf("provide a code directory or source file")
		}
		if err := filepath.WalkDir(input.Root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			name := entry.Name()
			if entry.Type()&os.ModeSymlink != 0 {
				if strings.HasPrefix(name, ".") || name == "vendor" || name == "testdata" || name == "node_modules" {
					return nil
				}
				info, statErr := os.Stat(path)
				if strings.HasSuffix(name, ".go") || statErr != nil || info.IsDir() {
					return fmt.Errorf("%s is a symlink; scan its source directory explicitly", path)
				}
				return nil
			}
			if entry.IsDir() {
				if path != input.Root && (strings.HasPrefix(name, ".") || name == "vendor" || name == "testdata" || name == "node_modules") {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go") {
				return parse(path, nil)
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	// Parse the whole package before scanning calls so globals, receiver types
	// and functions declared in sibling files resolve to their actual objects.
	for _, files := range packages {
		objects := map[string]*ast.Object{}
		for _, file := range files {
			for name, object := range file.Scope.Objects {
				if previous := objects[name]; previous != nil && previous != object && name != "init" && name != "_" {
					return nil, fmt.Errorf("%s: duplicate package declaration %s", a.positions.Position(file.Pos()).Filename, name)
				}
				objects[name] = object
			}
		}
		for _, file := range files {
			for _, name := range file.Unresolved {
				if object := objects[name.Name]; object != nil {
					name.Obj = object
				}
			}
		}
		for _, file := range files {
			a.bind(file)
		}
		for _, file := range files {
			sites = append(sites, a.scan(file)...)
		}
	}
	sort.Slice(sites, func(i, j int) bool {
		if sites[i].File != sites[j].File {
			return sites[i].File < sites[j].File
		}
		if sites[i].Line != sites[j].Line {
			return sites[i].Line < sites[j].Line
		}
		return sites[i].Entity < sites[j].Entity
	})
	return sites, nil
}

func main() {
	var input request
	if err := json.NewDecoder(os.Stdin).Decode(&input); err != nil {
		fmt.Fprintln(os.Stderr, "decode analyzer request:", err)
		os.Exit(1)
	}
	sites, err := analyze(input)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := json.NewEncoder(os.Stdout).Encode(sites); err != nil {
		fmt.Fprintln(os.Stderr, "encode analyzer result:", err)
		os.Exit(1)
	}
}
