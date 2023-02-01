# Lucia

### Prerequisites

- Python 3.10.9

### Virtual Environment

```
make venv
source venv/bin/activate
```

### Install Requirements

```
make install
```

### Compile Requirements

_NOTE_ use only when adding a new dependency

```
make compile
```

### Local DB Setup

This following command will start a local postgres instance listening on port `5432`.

```
make db
```

To stop the local postgres instance, run the following command

```
make db-stop
```

### Run Development Server

If you want to connect to the local postgres instance, run the following command.

```
MODE=production make run
```

otherwise, the app will run with an in-memory sqlite database.

```
make run
```

### Run Tests

```
make test
```

### Run Linter

```
make lint
```
