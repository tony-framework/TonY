# Tony History Server

## Development
- Rename `conf/application.example.conf` to `conf/application.conf` and set 
all the necessary configurations.

**Note:** You can use the `gradlew` script in `tony` root. 
Make sure you allow execution permission (`chmod +x gradlew`) to script before running if encountering permission issue.
Assuming you have `gradlew` in `tony-history-server` folder and you are currently in `tony-history-server`:

- To build the project:
```
$ ./gradlew build
```

- To run the app:
```
$ ./gradlew runPlayBinary
```
**Note:** this will only reload when receiving new request, __not__ when file changes. For reloading on file changes (hot reloading):
```
$ ./gradlew runPlayBinary -t
```

- After the message `<=============> 100% EXECUTING ...` displays, go to <http://localhost:9000> on your browser to see the app.

- To run tests:
```
$ ./gradlew testPlayBinary
```

For more info about developing with Play using Gradle, click [here](https://docs.gradle.org/current/userguide/play_plugin.html#play_continuous_build).

## Production
- Bundle the production binary:
```
$ ./gradlew dist
```

Before running the production binary, all the configurations for Tony History Server should be 
set in `$TONY_CONF_DIR/tony-site.xml`. You can look at the [sample](./conf/tony-site.sample.xml) 
to set up your own `tony-site.xml`.

- To start the server after bundling with `./gradlew dist`:
1. `unzip $root/build/distributions/tony-history-server.zip`
2. `$root/build/distributions/tony-history-server/bin/startTHS.sh`

See [script](./startTHS.sh) for more details.

### Deployment

Before using the script, ensure that you have set execution permission (`chmod +x buildAndDeploy.sh`)

- To bundle the history server app and deploy to another host
```
$ ./buildAndDeploy.sh user hostname.test.abc.com
```

See [script](./buildAndDeploy.sh) for more details.
