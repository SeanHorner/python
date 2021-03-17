import time

# #########################################          Variables             ############################################
secret_word_list = [
    "abruptly", "absurd", "abyss", "affix", "askew", "avenue", "awkward", "axiom",
    "azure", "bagpipes", "bandwagon", "banjo", "bayou", "beekeeper", "bikini", "blitz", "blizzard", "boggle",
    "bookworm", "boxcar", "boxful", "buckaroo", "buffalo", "buffoon", "buxom", "buzzard", "buzzing", "buzzwords",
    "caliph", "cobweb", "cockiness", "croquet", "crypt", "curacao", "cycle", "daiquiri", "dirndl", "disavow",
    "dizzying", "duplex", "dwarves", "embezzle", "equip", "espionage", "euouae", "exodus", "faking", "fishhook",
    "fixable", "fjord", "flapjack", "flopping", "fluffiness", "flyby", "foxglove", "frazzled", "frizzled", "fuchsia",
    "funny", "gabby", "galaxy", "galvanize", "gazebo", "giaour", "gizmo", "glowworm", "glyph", "gnarly", "gnostic",
    "gossip", "grogginess", "haiku", "haphazard", "hyphen", "iatrogenic", "icebox", "injury", "ivory", "ivy", "jackpot",
    "jaundice", "jawbreaker", "jaywalk", "jazziest", "jazzy", "jelly", "jigsaw", "jinx", "jiujitsu", "jockey",
    "jogging", "joking", "jovial", "joyful", "juicy", "jukebox", "jumbo", "kayak", "kazoo", "keyhole", "khaki",
    "kilobyte", "kiosk", "kitsch", "kiwifruit", "klutz", "knapsack", "larynx", "lengths", "lucky", "luxury", "lymph",
    "marquis", "matrix", "megahertz", "microwave", "mnemonic", "mystify", "naphtha", "nightclub", "nowadays",
    "numbskull", "nymph", "onyx", "ovary", "oxidize", "oxygen", "pajama", "peekaboo", "phlegm", "pixel", "pizazz",
    "pneumonia", "polka", "pshaw", "psyche", "puppy", "puzzling", "quartz", "queue", "quips", "quixotic", "quiz",
    "quizzes", "quorum", "razzmatazz", "rhubarb", "rhythm", "rickshaw", "schnapps", "scratch", "shiv", "snazzy",
    "sphinx", "spritz", "squawk", "staff", "strength", "strengths", "stretch", "stronghold", "stymied", "subway",
    "swivel", "syndrome", "thriftless", "thumbscrew", "topaz", "transcript", "transgress", "transplant", "triphthong",
    "twelfth", "twelfths", "unknown", "unworthy", "unzip", "uptown", "vaporize", "vixen", "vodka", "voodoo", "vortex",
    "voyeurism", "walkway", "waltz", "wave", "wavy", "waxy", "wellspring", "wheezy", "whiskey", "whizzing", "whomever",
    "wimpy", "witchcraft", "wizard", "woozy", "wristwatch", "wyvern", "xylophone", "yachtsman", "yippee", "yoked",
    "youthful", "yummy", "zephyr", "zigzag", "zigzagging", "zilch", "zipper", "zodiac", "zombie"]

randNum = int(time.time() % len(secret_word_list))

secret_word = secret_word_list[randNum]
guess_word = ""


# #########################################          Functions             ############################################
def check_exact(guess):
    count = 0
    i = 0

    while i < len(secret_word) and i < len(guess):
        if secret_word[i] == guess[i]:
            count += 1
        i += 1

    return count


def check_in_word(guess):
    count = 0
    guess_set = []
    secret_set = []

    for c in guess:
        guess_set.append(c)
    for c in secret_word:
        secret_set.append(c)

    for g in guess_set:
        for s in secret_set:
            if g == s:
                count += 1
                secret_set.remove(s)

    guess_set.clear()
    secret_set.clear()

    return count


# #########################################          Main Code             ############################################
print("The secret word is " + str(len(secret_word)) + " letters long.\n")

while guess_word != secret_word:
    guess_word = input("Enter your guess: ")
    if guess_word == secret_word:
        print("You guessed the word! Congratulations!")
    else:
        print(f"You have {check_in_word(guess_word)} correct letters,"
              f" with {check_exact(guess_word)} of them in the correct place.\n")
