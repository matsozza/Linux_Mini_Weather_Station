VERSION=1:0:0

# Compiler and Flags
CROSS_COMPILE=aarch64-linux-gnu-
CC=$(CROSS_COMPILE)gcc
CFLAGS=-Wall -Wextra -ggdb3 -I./include -pthread
LDFLAGS=-static -L./lib 
LIBS=

# Directories
SRC_DIR:=./src

# Python files
PY_FILES = $(wildcard $(SRC_DIR)/*.py)

# Target destination
#TAR_DEV := rpi.local
TAR_DEV := 192.168.0.78
TAR_DEST := ~

# Filenames
PY_NAME:=weather-station

# File Names
SRC_FILES:=$(wildcard $(SRC_DIR)/*.py)

all:
	@echo "\n--------------------------------------------------------------------------------"
	@echo "Compiling submodule 1 - DHT22" | fold -w 80
	@echo "--------------------------------------------------------------------------------"
	$(MAKE) -C ./submodules/linux_rpi ARCH=arm64 CROSS_COMPILE=aarch64-rpi3-linux-gnu- bcm2711_defconfig
	$(MAKE) -C ./submodules/linux_rpi ARCH=arm64 CROSS_COMPILE=aarch64-rpi3-linux-gnu- prepare
	$(MAKE) -C ./submodules/linux_rpi ARCH=arm64 CROSS_COMPILE=aarch64-rpi3-linux-gnu- modules_prepare
	$(MAKE) -C ./submodules/dht22 all
	$(MAKE) -C ./submodules/dht22 install

	@echo "\n--------------------------------------------------------------------------------"
	@echo "Compiling submodule 2 - BMP280" | fold -w 80
	@echo "--------------------------------------------------------------------------------"
	$(MAKE) -C ./submodules/bmp280

	@echo "\n--------------------------------------------------------------------------------"
	@echo "Moving Weather Station files to Target" | fold -w 80
	@echo "--------------------------------------------------------------------------------"
	scp $(PY_FILES) $(TAR_DEV):$(TAR_DEST)

# Rule to test file into target
test: $(BIN_FILES)
	@echo "\n------------------------------------------------"
	@echo "Sending submodule 1 files to the TARGET" | fold -w 48
	@echo "------------------------------------------------"
	$(MAKE) -C ./submodules/dht22 python

	@echo "\n------------------------------------------------"
	@echo "Sending submodule 2 files to the TARGET" | fold -w 48
	@echo "------------------------------------------------"
	$(MAKE) -C ./submodules/bmp280 test

	@echo "\n------------------------------------------------"
	@echo "Sending test files to the TARGET" | fold -w 48
	@echo "------------------------------------------------"
	scp $(PY_FILES) $(TAR_DEV):$(TAR_DEST)

	@echo "\n------------------------------------------------"
	@echo "Running test files" | fold -w 48
	@echo "------------------------------------------------"
	ssh $(TAR_DEV) 'sudo python -u ./weather-station.py'

	@echo "\n------------------------------------------------"
	@echo "DONE!" | fold -w 48
	@echo "------------------------------------------------"

# Clean rule to remove object files and binaries
clean:
	@echo "\n------------------------------------------------"
	@echo "Cleaning all previous build files (+submodules)   "
	@echo "------------------------------------------------"
	$(MAKE) -C ./submodules/linux_rpi ARCH=arm64 CROSS_COMPILE=aarch64-rpi3-linux-gnu- clean
	$(MAKE) -C ./submodules/dht22 clean
	rm -rf $(OBJ_DIR) $(BIN_DIR)

# Rule for phony BIN_FILESs (not files)
.PHONY: all clean test