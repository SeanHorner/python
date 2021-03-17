def power(a, b):
    if b > 1:
        result = 1
        for i in range(int(b)):
            result *= a
        return result
    elif b < 0:
        result = 1.0
        for i in range(int(b)):
            result /= a
        return result
    else:
        print("Currently not capable of fractional powers.")

proceed = True

while(proceed):
    num1 = float(input("Enter a number: "))
    num2 = float(input("Enter second number: "))
    oper = input("Enter operator (+, - , *, /, ^, Quit): ")

    while(True):
        if oper == "+":
            print(num1 + num2)
            break
        elif oper == "-":
            print(num1 - num2)
            break
        elif oper == "*":
            print(num1 * num2)
            break
        elif oper == "/":
            print(num1 / num2)
            break
        elif oper == "^":
            print(power(num1, num2))
            break
        elif oper == "Quit" or oper == "quit":
            proceed = False
            break
        else:
            print("Incorrect operator choice")
            oper = input("Please enter a correct option:")

    print()
