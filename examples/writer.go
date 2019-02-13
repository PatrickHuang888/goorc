package main

import (
	"github.com/PatrickHuang888/goorc/orc"
)

func main() {
	x := &orc.TypeDescription{Category: orc.INT}
	y := &orc.TypeDescription{Category: orc.STRING}
	children := []*orc.TypeDescription{x, y}
	names := []string{"x", "y"}
	schema := &orc.TypeDescription{Category: orc.STRUCT, Children: children, FieldNames: names}

}
