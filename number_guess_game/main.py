# * 	Creator:	Sean Horner
# * 	Date:		09/02/2021
# *     Updated:	09/02/2021
# * 	Purpose:	A simple text game that asks users to guess combinations of numbers
# * 				given a few hints. This code practices a few alternative methods to
# * 				perform a switch code block (i.e. dictionary and if-elif chain
# * 				methods).
# * 	Requires:	None


from random import randint


def game(level: int):
    if level == 1:
        # First level message: Gazorpazorp
        print(
            """
    * As you burst out of the portal, you enter a dimly lit room, LEDs and text *
    * displays glow in the dark. You see Rick engaged in dire battle with an    *
    * enraged Gazorpazorpian!                                                   *
            
    Rick:  Quick! Morty! These Gazorpazorpians are real f*ckin idiots! The code
           is probably written down somewhere!
    Morty: Oh crap, Rick! Okay, um, uh...
            
    * You quickly glance around and see a conspicuous Post-It note on one of    *
    * the computers. It reads:                                                  *  
            """)
    elif level == 2:
        # Second level message: Intergalactic Federation base
        print(
            """
    * As you spill out of that portal, you hear laser blasts all around you!    *
    * You've tumbled into a full-on brawl aboard an Intergalactic Federation    *
    * base station!                                                             *
            
    Rick:  Damn! Morty, I'm pinned down! See if you can roll over to the
           console and--
            
    * Suddenly a blast rips through the metal just next to Rick's ear, cutting  *
    * him off.                                                                  *
            
    Morty: Aw, geez, Rick! There's like 50 guys trying to shoot us! What if
           I get shot?!
    Rick:  Well then let's make it easy! If you don't do it, I'll shoot you!
            
    * You'd like to think that Rick is being overly-dramatic, but that didn't   *
    * sound very sarcastic. During a brief lull in the firing (reloading, you   *
    * hope) you dive for the console. Successfully there, you read:             *
            """)
    elif level == 3:
        # Third level message: the Vindicators' space station
        print(
            """
    * You spill out of the portal into yet another dimly-lit room. But this one *
    * seems familiar to you...                                                  *
            
    Morty: Rick? Rick?
    Rick:  Morty! Watch out!
            
    * Just as Rick shouts, a tiny galaxy shoots past your head with a woosh.    *
    * Suddenly you feel an invisible grip wrap around your throat. Scanning     *
    * the room for your assailant you lock eyes with two glowing, purple orbs   *
    * in the corner of the room. It's Supernova! Rick rolls out from his hiding *
    * place and hits her flank with a blast of science-y light. You fell the    *
    * throttling grip disappear.                                                *
            
    Rick:  Quick, Morty! The console's over there.
            
    * Rick points to a nearby console. You know the drill, you make your way    *
    * over while avoiding the mayhem spewing from the Rick-Supernova clash.     *
            """)
    elif level == 4:
        # Fourth level message: Old Man/the Devil's shop [Sphinx]
        print(
            """
    * You both fall out of the portal nearly simultaneously. This room is dusty *
    * and full of wooden shelves covered with random minutiae...                *
            
    Devil: Well hello Rick, we meet again...
            
    * The Devil slowly hobbles out from a shadow, aided by a cane. His face     *
    * twisting tortuously with every step.                                      *
            
    Rick:  Oh damn! I thought you'd, like, super heal yourself or something
           after we messed you up.
    Devil: I only get so many mortal coils per century, I'm not going to
           waste this one just because you put a few dents in it.
            
    * You decide to leave the two to catch up as you search out the console you *
    * know must be here. You begin searching the room, but everything in        *
    * here is an antique. You don't see a computer screen anywhere. About       *
    * to give up, you notice something odd. Hanging in the air above a nearby   *
    * type writer, is if typed on invisible paper, you see:                     *
            """)
    elif level == 5:
        # Final level message: the Zigerion scammers' VR machine
        print(
            """
    * You slowly open your eyes. Feels like you've been asleep for awhile. And  *
    * you are in your bed, wearing your pajamas. Wait was it all a dream? you   *
    * wonder. That would actually explain a lot, I mean adventures with Rick    *
    * are more often crazy than not. You get out of bed and begin to get ready  *
    * ready for school. Looking out the window, it looks like it's gonna be a   *
    * good day. The weather looks perfect. The sun is bright. The pop-tart that *
    * lives across the is hopping into his toaster car to head to work. Wait... *
    * From downstairs you hear Rick shouting:                                   *
            
    Rick:  Morty! It's the Zigerions again!
            
    * You hear him running up the stairs                                        *
            
    Rick:  Quick Morty, I patched your laptop into the system, quick before     *
           they shut you out!
            
    * You open your laptop screen. Instead of your normal background, Jessica's *
    * summer vacation Instagram posts, it's just a simple console, reading:     *
            """)
    else:
        print("How did you get here?\n\n")


def success(level: int):
    if level == 1:
        print(
            """
    * Success! The computer logs in just as Rick finally bests the              *
    * battle-hardened Gazorpazorpian. Rick heads over to the console and puts   *
    * in a few lines of code.                                                   *
            
    Rick:  Alright Morty, on to the next junction point!
    Morty: Right! Wait... what are we doing again?
    Rick:  Dammit, Morty! There's no time!
            
    * Rick shouts as he opens and immediately jumps through another portal.     *
    * Aw geez, you think as you scramble after him.                             *
            """)
    elif level == 2:
        print("""
    * Success! You're logged in! After Rick mops up the rest of the Federation  *
    * soldiers, he jogs over to the console and enters a few commands.          *
            
    Rick:  Excellent, Morty! Really, just a real *burp* bang-up job!
           On to the next!
            
    * Rick opens another portal and leaps through.                              *
            
    Morty: Wait! Rick! Dammit, what's going on?!
            
    * You reluctantly follow Rick through the portal.                           *
            """)
    elif level == 3:
        print(
            """
    * Success! The console logs into the server. You turn around in time to see *
    * Rick being menaced by Supernova with a micro-sized Jupiter, his portal    *
    * gun a few feet out of his reach. Just before she brings the micro-planet  *
    * down to crush Rick, you shout                                             *
            
    Morty: Uh - hey- uh- Supernova, what can kill a galaxy?
            
    * As she turns in your direction, her expression changes from one of        *
    * conquest to one of apprehension as she sees you holding the portal gun.   *
            
    Morty: A black hole!
            
    * You shout as you launch a portal right behind her to what you're pretty   *
    * sure is the all black holes dimension. Or maybe it's the dimension where  *
    * everybody takes really deep breaths. Either way, in spinning to look at   *
    * the portal, Supernova's hand passes through the barrier, causing her to   *
    * be sucked inside!
            
    Rick: Nicely done, Morty! Only two more to go!
            
    * Rick takes his portal gun, opens a new portal and hops through. As you    *
    * follow after him you wonder how long your portal will remain open and if  *
    * it might accidentally eat up anything else. Eh, no time for that now.     *
            """)
    elif level == 4:
        print(
            """
    * Success! As you press the carriage return the final time, the invisible   *
    * paper un-spools and floats off somewhere. You turn around and, much to    *
    * your surprise, you see Rick and the Devil chilling out on some beanbags   *
    * listening to some record on vinyl.                                        *
            
    Morty: Wait, wha?"
    Rick:  It's all good Morty, we figured no need ride this carousel again.
            
    * As Rick finishes his statement, he passes his flask to the Devil, who     *
    * who takes a long swig from it.                                            *
            
    Devil: Yeah, I'm not about futility, that's for the poor saps I punish.
    Morty: Uh, okay? So we're all cool? Rick, this doesn't seem weird to you?
    Rick:  I mean, any weirder than normal, no not really.
            
    * He has a good point. Rick takes his flask back, gets up off the pseudo-   *
    * chair, pulls out his portal gun, and opens a new portal.                  *
            
    Rick:  Alright morty, one final junction point to go.
            
    * You and Rick jump through the portal                                      *
            """)
    elif level == 5:
        print(
            """
    Morty:  Okay, now *this* seems really weird Rick, are we in a dream?
    Rick:   Yes Morty, it would seem that as in all poorly-written fiction,
            we're just now coming out of a Dallas-esque "everything is okay"
            dream sequence What a load!
            
    * Slowly you feel your eyes begin to open. Yup, you're in your bed, in      *
    * your pajamas, and running 10 minutes late for school! "Aw geez!" you      *
    * shout as you run down the stairs half-dressed. Slowly, Rick rises from    *
    * his hiding place below your bed, removing a Dreamalizer from his ear.     *
            
    Congratulations, you beat my ridiculous game! Please let me know if you
    liked it, and feel free to fork it into your own version, just let me
    know so I can try it :D!
            """)
    else:
        print("How did you get here?\n\n")


def failure(level: int):
    if level == 1:
        print(
            """
    * Wrong! The computer shuts down with a loud siren. You've tripped the      *
    * alarm. Rick turns his head to chastise you for your idiocy when suddenly  *
    * the tip of a Gazorpazorpian spear bursts forth from Rick's chest,         *
    * wielded by another warrior entering the room.                             *
            """)
    elif level == 2:
        print(
            """
    * Failure! Through a screaming siren, the console declares:                 *
            
    Console:    Incorrect password entry, self-destruct mechanism activated,
                detonating in two--
    Morty:      wait wha--?!
    Console:    One. Destruct--
            
    * The computer console explodes, rending a hole in the exterior hull,       *
    * sucking you, Rick, and a good number of Federation soldiers out into the  *
    * endless void.                                                             *
            """)
    elif level == 3:
        print(
            """
    * Failure! But the console offers you another chance. It reads:             *
            
        *** To try again, press enter ***
            
    * As you go to press the enter key, a softball size comet whips past your   *
    * head straight into the console screen. As the console showers you in      *
    * sparks, you turn to Rick. As soon as you do, you feel the invisible hand  *
    * at your throat again, this time much stronger and much more sure of your  *
    * demise. You slowly feel the world go dark as you hear Supernova's cackle  *
    * ringing in your ears...                                                   *
            """)
    elif level == 4:
        print(
            """
    * Wrong! The typewriter disappears in a puff of smoke! You turn to Rick to  *
    * protest, but he's not there! Nor is the Devil?! In fact, there's nothing  *
    * at all in the room. There is no room. You're suddenly in an inky black    *
    * void, surrounded by nothingness. The Devil has banished you to the void!  *
            """)
    elif level == 5:
        print(
            """
    * Sirens begin blaring. Suddenly, you snap awake in your bed. Wait, what?   *
    * you think to yourself. Was it all a dream? Well that doesn't matter right *
    * now because school started 10 minutes ago! You launch yourself out of bed *
    * looking for your one clean shirt: the yellow one. Throwing it on, you run *
    * out the door, snagging your backpack on the way out. Hopefully today will *
    * be a normal day, you think. The algorithm says it will.                   *
            """)
    else:
        print("How did you get here?\n\n")


def nth(n: int) -> str:
    switch_block = {
        1: "first",
        2: "second",
        3: "third",
        4: "fourth"
    }
    return switch_block.get(n, "How did you get here?")


def that_is_a_letter_morty():
    print(
        """
    * Peeking over your shoulder, right at this moment Rick *
    * sighs exasperatedly and says:                         *
        
    Rick:  Morty, you idiot, that's not a number!
        """)


def guess_input(prompt: str) -> int:
    try:
        guess = int(input(prompt))
    except ValueError:
        that_is_a_letter_morty()
        return guess_input("* A number this time, please: ")
    else:
        return guess


def main():
    nums = list()
    guesses = list()

    level = 1

    proceed = True

    print("\n\n")
    while proceed and level <= 5:
        # Print the level text
        game(level)

        # Generating random numbers for level
        i = 0
        while i < level * 2:
            nums.append(randint(1, level))
            i += 3

        # Calculating the sum and product of the random numbers
        summage = 0
        prod = 1
        for i in nums:
            summage += i
            prod *= i

        # Presenting the number hints
        print(
            """
    ~ There are {} numbers
    ~ Their sum is {}
    ~ Their product is {}
            """.format(len(nums), summage, prod))

        # Prompt the player for guesses
        print(
            """
    * The screen displays another message:
            """)

        # Collecting guess into guesses list, the function implements a try-catch block
        # and recursion to ensure that only integers are entered.
        for i in range(1, len(nums) + 1):
            guesses.append(guess_input(f"\tEnter your {nth(i)} number: "))

        # Sorting both lists, so if they contain the same numbers they should become identical
        nums.sort()
        guesses.sort()

        # Checking both lists items for parity
        for i in range(len(nums)):
            if not nums[i] == guesses[i]:
                proceed = False

        # Emptying the lists for the next round
        nums.clear()
        guesses.clear()

        if proceed:
            success(level)
            level += 1
        else:
            failure(level)

        if level > 5 or not proceed:
            c = 'y'
            print("\tWould you like to play again? (y/n): ", end='')
            c = input()

            if c[0] == 'y' or c[0] == 'Y':
                level = 1
                proceed = True


if __name__ == '__main__':
    main()
