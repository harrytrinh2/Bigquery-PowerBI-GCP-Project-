def swap(A):
    for i in range(0,len(A),1):
        for j in range(i+1,len(A),1):
            if A[j] < A[i]:
                tempt = A[j]
                A[j]  = A[i]
                A[i]  = tempt
    print(A)
if __name__ == "__main__":
    B = [8,3,4,5]
    swap(B)