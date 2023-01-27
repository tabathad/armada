package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/armadaproject/armada/internal/scheduler"
)

func runCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Runs the scheduler",
		RunE:  runScheduler,
	}
	return cmd
}

func runScheduler(_ *cobra.Command, _ []string) error {
	userSpecifiedConfigs := viper.GetStringSlice(CustomConfigLocation)
	println(userSpecifiedConfigs)
	config, err := loadConfig()
	if err != nil {
		return err
	}
	return scheduler.Run(config)
}
