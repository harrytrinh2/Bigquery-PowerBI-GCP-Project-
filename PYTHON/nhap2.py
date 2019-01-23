data_rows = 300
_arr = []
for i_row in range(data_rows):
    _arr.append(i_row)
    if i_row != 0 and i_row % 1500 == 0:
        print("Insert")
        _arr = []
print(_arr)
