lower_case = "abcdefghijklmnopqrstuvwxyz"
upper_case = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
results = "\n"


def count_char(line, char):
    count: int = 0
    total_length = len(line)
    for c in line:
        if c == char:
            count += 1
    return (count / total_length) * 100


filename = input("Enter name of file to be parsed: ")
with open(filename) as f:
    text = f.read().replace(" ", "")

for i in range(0, 25):
    upper = upper_case[i]
    lower = lower_case[i]
    capital = count_char(text, upper)
    base = count_char(text, lower)
    results += f"Letter: {upper}\tCapital: {capital:.2f}%\tLower: {base:.2f}%\tTotal: {(capital + base):.2f}%\n"

print(results)
