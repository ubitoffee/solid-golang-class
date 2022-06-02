package cli

var Args struct {
	LoggingToFileEnabled bool   `arg:"env:EDP_ENABLE_LOGGING_TO_FILE,-l,--logToFile" default:"false" help:"Enable logging to file"`
	LogfilePath          string `arg:"env:EDP_LOGFILE_PATH,-f,--logfilePath" default:"logs/event-log-collector.log" help:"Location and name of file to log to"`
	DebugEnabled         bool   `arg:"env:EDP_ENABLE_DEBUG_LOGGING,-d,--debug" help:"Specify this flag to enable debug logging level"`
	Config               string `arg:"env:EDP_CONFIG,-c,--config" default:"configs/" help:"Path to event logger configs. Can be either a directory or specific json config file"`

	Port               int    `arg:"env:EDP_PORT,-p,--port" default:"8078" help:"Port for the service to listen on"`
	Addr               string `arg:"env:EDP_ADDRESS,-a,--addr" default:"localhost" help:"Address of the service"`
	Scheme             string `arg:"env:EDP_SCHEME,-s,--scheme" default:"http" help:"Scheme of the service (http or https"`
	ServerReadTimeout  int    `arg:"env:EDP_SERVER_READ_TIMEOUT,--serverReadTimeout" default:"60" help:"Server read timeout in seconds"`
	ServerWriteTimeout int    `arg:"env:EDP_SERVER_WRITE_TIMEOUT,--serverWriteTimeout" default:"60" help:"Server write timeout in seconds"`
	BasePath           string `arg:"env:EDP_BASE_PATH,--basePath" default:"" help:"Base path to prefix api routes. Use this when deployed behind a reverse proxy"`
}
