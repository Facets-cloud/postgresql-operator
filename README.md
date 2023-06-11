# PostgreSQL Operator

The PostgreSQL Operator is a tool designed to simplify the management of users and permissions for PostgreSQL databases. It is built on top of Kubernetes, a popular container orchestration platform, and leverages its capabilities for managing PostgreSQL Users.

One of the key features of the PostgreSQL Operator is its ability to automate user and permission management. You can define custom resources to represent database users and their associated privileges. The operator then takes care of creating and managing these users within the PostgreSQL clusters based on the desired configuration.

By using the PostgreSQL Operator, you can streamline the process of managing user access and permissions across multiple databases and clusters. It provides a centralized and consistent approach to user management, making it easier to enforce security policies and ensure the integrity of your data

## Getting Started

This guide provides an introduction to using the PostgreSQL Operator. It will help you get started with the basics of utilizing this operator effectively

### Pre-requisites

- You’ll need a Kubernetes cluster to run against. You can use [KIND](https://sigs.k8s.io/kind) to get a local cluster for testing, or run against a remote cluster.
**Note:** Your controller will automatically use the current context in your kubeconfig file (i.e. whatever cluster `kubectl cluster-info` shows).
- A kubernetes secret that contains base64 encrypted PostgreSQL Database details `username`, `password`, `endpoint`, `port`, `database` and `role_password`
  > _Note:_
  > - You can use existing secret with database details and role password
  > - You can new secret with database details and role password
  > - You can also created two separate secret for database details and role password

  - Create a secret that contains both the database details and the role password. You have the flexibility to choose your own name for the key representing the role password, as long as you reference it correctly in the Role CRD.

    ```bash
    kubectl create secret generic <secret_name> --from-literal=username=<postgresql_username> --from-literal=password=<postgresql_password> --from-literal=endpoint=<postgresql_endpoint> --from-literal=port=<postgresql_port> --from-literal=database=<postgresql_database> --from-literal=role_password=<postgresql_role_password>
    ```

### Usage

Here are the CRD manifests for creating a PostgreSQL Role and Grant. For more detailed information about the APIs, please refer to this [doc](docs/crd.md)

#### Example Role CRD

```yaml
apiVersion: postgresql.facets.cloud/v1alpha1
kind: Role
metadata:
  name: test-role
spec:
  connectSecretRef:
    name: db-conn
    namespace: default
  passwordSecretRef:
    namespace: default
    name: db-conn
    key: role_password
  connectionLimit: 100
  privileges:
    bypassRls: false
    createDb: false
    createRole: false
    inherit: false
    login: true
    replication: false
    superUser: false
```

#### Example Grant CRD

```yaml
apiVersion: postgresql.facets.cloud/v1alpha1
kind: Grant
metadata:
  name: test-grant
spec:
  roleRef:
    name: test-role
    namespace: default
  privileges:
    - INSERT
    - UPDATE
  database: test
  schema: public
  table: ALL
```

For more examples, kindly check [here](examples)

### Running on the cluster

1. Install Instances of Custom Resources:

```sh
kubectl apply -f config/samples/
```

2. Build and push your image to the location specified by `IMG`:

```sh
make docker-build docker-push IMG=<some-registry>/postgresql-operator:tag
```

3. Deploy the controller to the cluster with the image specified by `IMG`:

```sh
make deploy IMG=<some-registry>/postgresql-operator:tag
```

### Uninstall CRDs

To delete the CRDs from the cluster:

```sh
make uninstall
```

### Undeploy controller

UnDeploy the controller from the cluster:

```sh
make undeploy
```

## Contributing

1. Fork the project and clone locally.
2. Create a branch with the changes.
3. Install Go version `1.20`
4. Test your [changes](#test-it-out)
5. Commit, push, and create a PR

### How it works

This project aims to follow the Kubernetes [Operator pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/).

It uses [Controllers](https://kubernetes.io/docs/concepts/architecture/controller/),
which provide a reconcile function responsible for synchronizing resources until the desired state is reached on the cluster.

### Test It Out

1. Install the CRDs into the cluster:

```sh
make install
```

2. Run your controller (this will run in the foreground, so switch to a new terminal if you want to leave it running):

```sh
make run
```

**NOTE:** You can also run this in one step by running: `make install run`

### Modifying the API definitions

If you are editing the API definitions, generate the manifests such as CRs or CRDs using:

```sh
make manifests
```

**NOTE:** Run `make --help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

Copyright 2023 Pramodh Ayyappan.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
