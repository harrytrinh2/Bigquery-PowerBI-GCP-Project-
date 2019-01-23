import os

def check_files(path):
    for file in os.listdir(path):
        if os.path.isfile(os.path.join(path, file)):
            yield file

for file in check_files("C:\\Users\\PhucCoi\\Documents\\PYTHON" + "\\"):
    print (file)