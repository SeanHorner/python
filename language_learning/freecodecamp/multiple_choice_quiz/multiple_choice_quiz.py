from Question import Question

question_prompts = [
    "What color are apples?\n (a) Red/Green\n (b) Purple\n (c) Orange\n\n: ",
    "What color are bananas?\n (a) Teal\n (b) Magenta\n (c) Yellow\n\n: ",
    "What color are strawberries?\n (a) Yellow\n (b) Red\n (c) Blue\n\n: "
]

questions = [
    Question(question_prompts[0], "a"),
    Question(question_prompts[1], "c"),
    Question(question_prompts[2], "b"),
]


def run_test(qs):
    score = 0
    total = len(qs)
    for question in qs:
        ans = input(question.prompt)
        if ans == question.answer:
            score += 1
    print(f"You got: {score}/{total} for {(score/total)*100:.2f}%")


run_test(questions)
