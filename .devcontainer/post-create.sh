#!/bin/bash

# Define success and error color codes
SUCCESS_COLOR="\e[32m"
ERROR_COLOR="\e[31m"
RESET_COLOR="\e[0m"

# Define success tracking variables
cmakeInstalled=false
protocInstalled=false
rustupToolchainNightlyInstalled=false


# Define installation functions
install_cmake() {
    echo -e "Installing CMake..."
    sudo apt-get update
    sudo apt-get install -y cmake > /dev/null 2>&1
    if [[ "$(cmake --version)" =~ "cmake version" ]]; then
        echo -e "${SUCCESS_COLOR}CMake installed successfully.${RESET_COLOR}"
        cmakeInstalled=true
    else
        echo -e "${ERROR_COLOR}CMake installation failed. Please install it manually.${RESET_COLOR}"
    fi
}


configure_python_environment() {
    if command -v python3 &> /dev/null; then
        echo -e "${SUCCESS_COLOR}Python 3 is installed${RESET_COLOR}"
        # Create a symbolic link from python to python3
        sudo ln -s /usr/bin/python3 /usr/bin/python
    else
        echo -e "${ERROR_COLOR}Python 3 is not installed. Please install it manually.${RESET_COLOR}"
    fi
}

install_protoc() {
    echo -e "Installing protoc..."
    PB_REL="https://github.com/protocolbuffers/protobuf/releases"
    curl -LO $PB_REL/download/v3.15.8/protoc-3.15.8-linux-x86_64.zip > /dev/null 2>&1
    unzip protoc-3.15.8-linux-x86_64.zip -d $HOME/.local > /dev/null 2>&1
    export PATH="$PATH:$HOME/.local/bin"
    if [[ "$(protoc --version)" =~ "libprotoc 3.15.8" ]]; then
        echo -e "${SUCCESS_COLOR}protoc installed successfully.${RESET_COLOR}"
        protocInstalled=true
    else
        echo -e "${ERROR_COLOR}protoc installation failed. Please install it manually.${RESET_COLOR}"
    fi

    # Clean up
    rm -f protoc-3.15.8-linux-x86_64.zip
}

install_rustup_toolchain_nightly() {
    echo -e "Installing Rustup nightly toolchain..."
    rustup toolchain install nightly > /dev/null 2>&1
    rustup component add rustfmt --toolchain nightly > /dev/null 2>&1
    if [[ "$(rustup toolchain list)" =~ "nightly" && "$(rustup component list --toolchain nightly | grep rustfmt)" =~ "installed" ]]; then
        echo -e "${SUCCESS_COLOR}Rustup nightly toolchain and rustfmt installed successfully.${RESET_COLOR}"
        rustupToolchainNightlyInstalled=true
    else
        echo -e "${ERROR_COLOR}Rustup nightly toolchain and/or rustfmt installation failed. Please install them manually.${RESET_COLOR}"
    fi
}

# Install tools
install_cmake
configure_python_environment
install_protoc
install_rustup_toolchain_nightly


# Check the success tracking variables
if $cmakeInstalled && $protocInstalled && $rustupToolchainNightlyInstalled; then
    echo -e "${SUCCESS_COLOR}All tools installed successfully.${RESET_COLOR}"
    echo "Useful commands:"
    echo "  - make test-all: Starts necessary Docker services and runs all tests."
    echo "  - make -k test-all docker-compose-down: The same as above, but tears down the Docker services after running all the tests."
    echo "  - make fmt: Runs formatter (requires the nightly toolchain to be installed by running rustup toolchain install nightly)."
    echo "  - make fix: Runs formatter and clippy checks."
    echo "  - make typos: Runs the spellcheck tool over the codebase (install by running cargo install typos)."
    echo "  - make build-docs: Builds docs."
    echo "  - make docker-compose-up: Starts Docker services."
    echo "  - make docker-compose-down: Stops Docker services."
    echo "  - make docker-compose-logs: Shows Docker logs."
else
    echo -e "${ERROR_COLOR}One or more tools failed to install. Please check the output for errors and install the failed tools manually.${RESET_COLOR}"
fi
