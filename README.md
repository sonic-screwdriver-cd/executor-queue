# Executor Router
[![Version][npm-image]][npm-url] ![Downloads][downloads-image] [![Build Status][status-image]][status-url] [![Open Issues][issues-image]][issues-url] [![Dependency Status][daviddm-image]][daviddm-url] ![License][license-image]

> A generic executor plugin that routes builds to a specified executor

An executor is an engine that is capable of running a set of docker containers together.

i.e. Jenkins, Kubernetes, ECS, Mesos

The executor router will allow multiple executors to be used in a Screwdriver cluster.

## Usage

```bash
npm install screwdriver-executor-router
```

### Interface

It will initialize any routers specified in the [default.yaml](https://github.com/screwdriver-cd/screwdriver/blob/master/config/default.yaml#L89-L119) under the `executor` keyword. To specify a default executor plugin, indicate it at the `plugin` keyword. If no default is specified, the first executor defined will be the default.

**Example executor yaml section:**
```
executor:
    plugin: k8s
    k8s:
      options:
        kubernetes:
            host: kubernetes.default
            token: sometokenhere
        launchVersion: stable
    docker:
      options:
        docker: {}
        launchVersion: stable
    k8s-vm:
      options:
        kubernetes:
            host: kubernetes.default
            token: sometokenhere
        launchVersion: stable
```

## Testing

```bash
npm test
```

## License

Code licensed under the BSD 3-Clause license. See LICENSE file for terms.

[npm-image]: https://img.shields.io/npm/v/screwdriver-executor-router.svg
[npm-url]: https://npmjs.org/package/screwdriver-executor-router
[downloads-image]: https://img.shields.io/npm/dt/screwdriver-executor-router.svg
[license-image]: https://img.shields.io/npm/l/screwdriver-executor-router.svg
[issues-image]: https://img.shields.io/github/issues/screwdriver-cd/executor-router.svg
[issues-url]: https://github.com/screwdriver-cd/executor-router/issues
[status-image]: https://cd.screwdriver.cd/pipelines/202/badge
[status-url]: https://cd.screwdriver.cd/pipelines/202
[daviddm-image]: https://david-dm.org/screwdriver-cd/executor-router.svg?theme=shields.io
[daviddm-url]: https://david-dm.org/screwdriver-cd/executor-router
