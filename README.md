## Getting Started

Welcome to the VS Code Java world. Here is a guideline to help you get started to write Java code in Visual Studio Code.

## Folder Structure

The workspace contains:

- `src`: the folder to maintain sources

Meanwhile, the compiled output files will be generated in the `out` folder.

> If you want to customize the folder structure, open `.vscode/settings.json` and update the related settings there.

## Compilation

To compile all source files into the `out` directory, run the following command in your terminal:

```bash
javac -d out $(find src -name "*.java")
```

If your shell supports recursive globbing (like `zsh` on macOS), you can also run:

```bash
javac -d out src/**/*.java
```

## Dependency Management

The `JAVA PROJECTS` view allows you to manage your dependencies. More details can be found [here](https://github.com/microsoft/vscode-java-dependency#manage-dependencies).
