package storages_providers

import (
	"event-data-pipeline/pkg/logger"
	"fmt"
	"strings"
)

type (
	jsonObj = map[string]interface{}
	jsonArr = []interface{}
)
type StorageProvider interface {
	Write(payload interface{}) (int, error)
}

type StorageProviderFactory func(config jsonObj) StorageProvider

var storageProviderFactories = make(map[string]StorageProviderFactory)

// Each storage implementation must Register itself
func Register(name string, factory StorageProviderFactory) {
	logger.Debugf("Registering storage factory for %s", name)
	if factory == nil {
		logger.Panicf("Storage factory %s does not exist.", name)
	}
	_, registered := storageProviderFactories[name]
	if registered {
		logger.Errorf("Storage factory %s already registered. Ignoring.", name)
	}
	storageProviderFactories[name] = factory
}

// CreateStorageProvider is a factory method that will create the named storage
func CreateStorageProvider(name string, config jsonObj) (StorageProvider, error) {

	factory, ok := storageProviderFactories[name]
	if !ok {
		// Factory has not been registered.
		// Make a list of all available datastore factories for logging.
		availableStorages := make([]string, 0)
		for k := range storageProviderFactories {
			availableStorages = append(availableStorages, k)
		}
		return nil, fmt.Errorf("invalid Storage name. Must be one of: %s", strings.Join(availableStorages, ", "))
	}

	// Run the factory with the configuration.
	return factory(config), nil
}
