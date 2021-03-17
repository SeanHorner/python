def power(a, b):
    result = 1
    for i in range(int(b)):
        result *= a

    return result


print(power(16, 15))
