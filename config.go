package exchange

import "runtime"

// Config holds service configuration (env-loaded). Redis is created via common in NewServer.
type Config struct {
	NumShards         int
	ETFTickers        []string
	FuturesTickerKeys []string
}

// LoadConfig loads configuration from environment and defaults.
func LoadConfig() Config {
	cfg := Config{
		NumShards: 2 * runtime.NumCPU(),
		ETFTickers: []string{
			"FUESSV50", "FUEMAV30", "FUEKIVFS", "FUEVFVND", "FUEVN100", "FUEMAVND",
			"FUESSVFL", "FUEKIV30", "E1VFVN30", "FUEKIVND", "FUEDCMID", "FUESSV30",
		},
		FuturesTickerKeys: []string{
			"VNC1_ticker", "VNC2_ticker", "VNC3_ticker", "VNC4_ticker",
			"VNO1_ticker", "VNO2_ticker", "VNO3_ticker", "VNO4_ticker",
		},
	}
	return cfg
}
