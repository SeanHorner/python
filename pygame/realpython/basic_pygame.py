# simple pygame program

# import and initialize the pygame library

import pygame
pygame.init()

# set up the screen drawing window
screen = pygame.display.set_mode([500, 500])

# run until the user asks to quit
running = True
while running:

    # has the user clicked the close window button?
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False

    # fill the background with white
    screen.fill((255, 255, 255))
    # screen.fill() takes either a list or tuple of RGB values

    # draw a solid blue circle in the center of the screen
    pygame.draw.circle(screen, (127, 127, 127), (255, 255), 75)
    pygame.draw.circle(screen, (0, 0, 255), (250, 250), 75)
    # pygame.draw is layer specific, that is to say things that are drawn first,
    # i.e. their draw declarations are declared first, are on a lower layer than
    # things that are drawn later, therefore items drawn last are on top of
    # items drawn before them.
    # draw.circle() takes 4 arguments,
    #   ~ window:   the window to draw to, here the one described screen
    #   ~ rbg:      a tuple of RGB values (0 - 255) to set item's color
    #   ~ position: a tuple of X-Y coordinates for the given window
    #   ~ radius:   radius of the circle (in px value)

    # flip the display (nothing happens without this command, no screen change!)
    pygame.display.flip()
    # display.flip() updates the game screen with new graphics.

# Done, time to exit
pygame.quit()
