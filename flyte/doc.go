// Package flyte is a Go SDK for launching and monitoring runs of tasks
// deployed on a Flyte control plane. It mirrors the remote-execution
// surface of the Python flyte SDK while staying idiomatic Go, and is designed
// to be embedded in services: import, Init once, then Run tasks.
//
// # Quick start
//
//	if err := flyte.Init(ctx, flyte.Config{
//	    Endpoint: "acme.example.com",
//	    Project:  "my-project",
//	    Domain:   "development",
//	}); err != nil {
//	    log.Fatal(err)
//	}
//	defer flyte.Close()
//
//	task, err := flyte.GetTask(ctx, flyte.TaskRef{Name: "my_env.my_task"}) // latest version
//	run, err := flyte.Run(ctx, task, flyte.Inputs{"x": 5})
//	fmt.Println(run.URL())
//	if err := run.Wait(ctx); err != nil {
//	    log.Fatal(err)
//	}
//	outputs, err := run.Outputs(ctx)
//
// # Authentication
//
// The default is the browser-based PKCE flow. For headless use, provide a
// platform API key (flyte.Config{APIKey: ...} or flyte.InitFromAPIKey), or OAuth2
// client credentials (ClientID plus ClientSecret / ClientSecretEnvVar /
// ClientSecretLocation). DeviceFlow and ExternalCommand flows are also
// available via Config.AuthType. Configuration can equally be loaded from a
// flytectl/uctl-style YAML file with flyte.InitFromConfig.
package flyte
