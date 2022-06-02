package main

import (
	"event-data-pipeline/cmd"
	"event-data-pipeline/pkg"
	"event-data-pipeline/pkg/config"
	"event-data-pipeline/pkg/logger"
	"fmt"

	"github.com/common-nighthawk/go-figure"
)

func main() {
	PrintLogo()
	logger.Setup()
	cfg := config.NewConfig()
	cmd.Run(*cfg)
}

func PrintLogo() {
	logo := figure.NewColorFigure("Youngstone", "", "green", true)
	logo.Print()
	class := figure.NewColorFigure("Week 1 - SOLID GO", "", "yellow", true)
	class.Print()
	ccssLite := figure.NewColorFigure("Event Data Pipeline", "", "blue", true)
	ccssLite.Print()
	version := figure.NewColorFigure(fmt.Sprintf("v%s", pkg.GetVersion()), "", "red", true)
	version.Print()
	fmt.Println()
}
