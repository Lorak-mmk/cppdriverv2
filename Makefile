EMPTY :=
SPACE := ${EMPTY} ${EMPTY}

ifndef SCYLLA_TEST_FILTER
SCYLLA_TEST_FILTER := $(subst ${SPACE},${EMPTY},AsyncTests.Integration_Cassandra_Close)
endif

ifndef CASSANDRA_TEST_FILTER
CASSANDRA_TEST_FILTER := $(subst ${SPACE},${EMPTY},AsyncTests.Integration_Cassandra_Close)
endif

ifndef CCM_COMMIT_ID
	export CCM_COMMIT_ID := 81076bce792a0fb3f2050e4c209a93e4a62ab55f
endif

ifndef SCYLLA_VERSION
	SCYLLA_VERSION := release:6.1.1
endif

ifndef CASSANDRA_VERSION
	CASSANDRA_VERSION := 3.11.17
endif

CURRENT_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
BUILD_DIR := "${CURRENT_DIR}build"
INTEGRATION_TEST_BIN := ${BUILD_DIR}/cassandra-integration-tests

clean:
	rm -rf "${BUILD_DIR}"

update-apt-cache-if-needed:
	@{\
		@sudo find /var/cache/apt -type f -mtime -1 2>/dev/null | grep -c "" 2>/dev/null | grep 0 >/dev/null 2>&1 || (\
			echo "Apt cache is outdated, update it.";\
			sudo apt-get update || true;\
		)\
	}

install-cargo-if-missing: update-apt-cache-if-needed
	@cargo --version >/dev/null 2>&1 || (\
		echo "Cargo not found in the system, install it.";\
		sudo apt-get install -y cargo;\
	)

install-valgrind-if-missing: update-apt-cache-if-needed
	@valgrind --version >/dev/null 2>&1 || (\
		echo "Valgrind not found in the system, install it.";\
		sudo snap install valgrind --classic;\
	)

install-ccm-if-missing:
	@ccm list >/dev/null 2>&1 || (\
		echo "CCM not found in the system, install it.";\
		pip3 install --user https://github.com/scylladb/scylla-ccm/archive/${CCM_COMMIT_ID}.zip;\
	)

install-ccm:
	@pip3 install --user https://github.com/scylladb/scylla-ccm/archive/${CCM_COMMIT_ID}.zip

install-java8-if-missing:
	@{\
		dpkg -l openjdk-8-jre >/dev/null 2>&1 && exit 0;\
		echo "Java 8 not found in the system, install it";\
		sudo apt install -y openjdk-8-jre;\
	}

install-build-dependencies: update-apt-cache-if-needed
	@sudo apt-get install -y libssl1.1 libuv1-dev libkrb5-dev libc6-dbg

install-bin-dependencies: update-apt-cache-if-needed
	@sudo apt-get install -y libssl1.1 libuv1-dev libkrb5-dev libc6-dbg

build-integration-test-bin:
	@{\
		echo "Building integration test binary to ${INTEGRATION_TEST_BIN}";\
  		mkdir "${BUILD_DIR}" >/dev/null 2>&1 || true;\
		cd "${BUILD_DIR}";\
		cmake -DCASS_BUILD_INTEGRATION_TESTS=ON .. && (make -j 4 || make);\
	}

build-integration-test-bin-if-missing:
	@{\
		[ -f "${INTEGRATION_TEST_BIN}" ] && exit 0;\
		echo "Integration test binary not found at ${INTEGRATION_TEST_BIN}, building it";\
		mkdir "${BUILD_DIR}" >/dev/null 2>&1 || true;\
		cd "${BUILD_DIR}";\
		cmake -DCASS_BUILD_INTEGRATION_TESTS=ON .. && (make -j 4 || make);\
	}

_update-rust-tooling:
	@echo "Run rustup update"
	@rustup update

check-cargo: install-cargo-if-missing _update-rust-tooling
	@echo "Running \"cargo check\" in ./scylla-rust-wrapper"
	@cd ${CURRENT_DIR}/scylla-rust-wrapper; cargo check

fix-cargo:
	@echo "Running \"cargo fix --verbose --all\" in ./scylla-rust-wrapper"
	@cd ${CURRENT_DIR}/scylla-rust-wrapper; cargo fix --verbose --all

check-cargo-clippy: install-cargo-if-missing _update-rust-tooling
	@echo "Running \"cargo clippy --verbose --all-targets -- -D warnings -Aclippy::uninlined_format_args\" in ./scylla-rust-wrapper"
	@cd ${CURRENT_DIR}/scylla-rust-wrapper; cargo clippy --verbose --all-targets -- -D warnings -Aclippy::uninlined_format_args

fix-cargo-clippy: install-cargo-if-missing _update-rust-tooling
	@echo "Running \"cargo clippy --verbose --all-targets --fix -- -D warnings -Aclippy::uninlined_format_args\" in ./scylla-rust-wrapper"
	@cd ${CURRENT_DIR}/scylla-rust-wrapper; cargo clippy --verbose --all-targets --fix -- -D warnings -Aclippy::uninlined_format_args

check-cargo-fmt: install-cargo-if-missing _update-rust-tooling
	@echo "Running \"cargo fmt --verbose --all -- --check\" in ./scylla-rust-wrapper"
	@cd ${CURRENT_DIR}/scylla-rust-wrapper; cargo fmt --verbose --all -- --check

fix-cargo-fmt: install-cargo-if-missing _update-rust-tooling
	@echo "Running \"cargo fmt --verbose --all\" in ./scylla-rust-wrapper"
	@cd ${CURRENT_DIR}/scylla-rust-wrapper; cargo fmt --verbose --all

check: check-cargo check-cargo-clippy check-cargo-fmt

fix: fix-cargo fix-cargo-clippy fix-cargo-fmt

prepare-integration-test: update-apt-cache-if-needed install-valgrind-if-missing install-cargo-if-missing _update-rust-tooling
	@sudo sh -c "echo 2097152 >> /proc/sys/fs/aio-max-nr"
	@dpkg -l libc6-dbg >/dev/null 2>&1 || sudo apt-get install -y libc6-dbg

download-ccm-scylla-image: install-ccm-if-missing
	@echo "Downloading scylla ${SCYLLA_VERSION} CCM image"
	@rm -rf /tmp/download-scylla.ccm || true
	@mkdir /tmp/download-scylla.ccm || true
	@ccm create ccm_1 -i 127.0.1. -n 3:0 -v "${SCYLLA_VERSION}" --scylla --config-dir=/tmp/download-scylla.ccm
	@rm -rf /tmp/download-scylla.ccm

download-ccm-cassandra-image: install-ccm-if-missing
	@echo "Downloading cassandra ${CASSANDRA_VERSION} CCM image"
	@rm -rf /tmp/download-cassandra.ccm || true
	@mkdir /tmp/download-cassandra.ccm || true
	@ccm create ccm_1 -i 127.0.1. -n 3:0 -v "${CASSANDRA_VERSION}" --config-dir=/tmp/download-cassandra.ccm
	@rm -rf /tmp/download-cassandra.ccm

run-test-integration-scylla: prepare-integration-test download-ccm-scylla-image
ifdef DONT_REBUILD_INTEGRATION_BIN
run-test-integration-scylla: build-integration-test-bin-if-missing
else
run-test-integration-scylla: build-integration-test-bin
endif
	@echo "Running integration tests on scylla ${SCYLLA_VERSION}"
	build/cassandra-integration-tests --scylla --version=${SCYLLA_VERSION} --category=CASSANDRA --verbose=ccm --gtest_filter="${SCYLLA_TEST_FILTER}"

run-test-integration-cassandra: prepare-integration-test download-ccm-cassandra-image install-java8-if-missing
ifdef DONT_REBUILD_INTEGRATION_BIN
run-test-integration-cassandra: build-integration-test-bin-if-missing
else
run-test-integration-cassandra: build-integration-test-bin
endif
	@echo "Running integration tests on cassandra ${CASSANDRA_VERSION}"
	build/cassandra-integration-tests --version=${CASSANDRA_VERSION} --category=CASSANDRA --verbose=ccm --gtest_filter="${CASSANDRA_TEST_FILTER}"

run-test-unit: install-cargo-if-missing _update-rust-tooling
	@cd ${CURRENT_DIR}/scylla-rust-wrapper; cargo test
