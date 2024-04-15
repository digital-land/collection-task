# collection-workflow

Repository to run the nightly process for a given collection. Produces a docker container which can be used by cloud services to run for any given collection.

## Running Locally

This repo allows the team to replicate this task locally to fix any bugs. It doesn't have any of it's own defined python code so testing is limited. The colleciton can be ran using docker or using the task directory and setting up local dependencies to run the code.

### Docker

Given that docker is installed, docker compose can be used to run the process for a given colleciton. Volumes are set up to view outputs but this can cause it to be slow running on some machines. This can be ran using:

```
make compose-up
```

The logs can be viewed via docker desktop or similar tools. Once it has ran you can run 

```
make compose-down
```

to remove the container and the images you created.

You can edit the collection by changing the environment variable in the docker compose file.