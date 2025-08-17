from bmp280.bmp280 import read_bmp280_pipe
from dht22_kernel.dht22 import read_dht22_data

if __name__ == "__main__":
    read_bmp280_pipe()
    read_dht22_data()
