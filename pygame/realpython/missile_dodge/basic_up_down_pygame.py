# Import the pygame module
import pygame as pg
# Import random for random numbers
import random

# Import pygame.locals to more easily access the input keys
from pygame.locals import (
    RLEACCEL,
    K_UP,
    K_DOWN,
    K_LEFT,
    K_RIGHT,
    K_ESCAPE,
    KEYDOWN,
    QUIT,
)

# Setup for sounds. Defaults are good.
pg.mixer.init()

# Initialize pygame
pg.init()

# Define constants for the screen width and height
SCREEN_WIDTH = 800
SCREEN_HEIGHT = 600


# Define a Player object by extending pygame.sprite.Sprite
# The surface drawn on the screen is now an attribute of 'player'
class Player(pg.sprite.Sprite):
    def __init__(self):
        super(Player, self).__init__()
        self.surf = pg.image.load("jet.png").convert()
        self.surf.set_colorkey((0, 0, 0), RLEACCEL)
        self.rect = self.surf.get_rect()

    def update(self, pressed_keys):
        if pressed_keys[K_UP]:
            self.rect.move_ip(0, -5)
            move_up_sound.play()
        if pressed_keys[K_DOWN]:
            self.rect.move_ip(0, 5)
            move_down_sound.play()
        if pressed_keys[K_LEFT]:
            self.rect.move_ip(-5, 0)
        if pressed_keys[K_RIGHT]:
            self.rect.move_ip(5, 0)

        if self.rect.left < 0:
            self.rect.left = 0
        if self.rect.right > SCREEN_WIDTH:
            self.rect.right = SCREEN_WIDTH
        if self.rect.top <= 0:
            self.rect.top = 0
        if self.rect.bottom > SCREEN_HEIGHT:
            self.rect.bottom = SCREEN_HEIGHT


# Define an enemy object by extending pygame.sprite.Sprite
# The surface you draw on the screen is now an attribute of 'enemy'
class Enemy(pg.sprite.Sprite):
    def __init__(self):
        super(Enemy, self).__init__()
        self.surf = pg.image.load("missile.png").convert()
        self.surf.set_colorkey((0, 0, 0), RLEACCEL)
        self.rect = self.surf.get_rect(
            center=(
                random.randint(SCREEN_WIDTH + 20, SCREEN_WIDTH + 100),
                random.randint(0, SCREEN_HEIGHT),
            )
        )
        self.speed = random.randint(5, 20)

    # Move the sprite based on speed
    # Remove the sprite when i passes the left edge of the screen
    def update(self):
        self.rect.move_ip(-self.speed, 0)
        if self.rect.right < 0:
            self.kill()


# Defining a cloud object
class Cloud(pg.sprite.Sprite):
    def __init__(self):
        super(Cloud, self).__init__()
        self.surf = pg.image.load("cloud.png").convert()
        self.surf.set_colorkey((0, 0, 0), RLEACCEL)
        # Randomly generated starting position
        self.rect = self.surf.get_rect(
            center=(
                random.randint(SCREEN_WIDTH + 20, SCREEN_WIDTH + 100),
                random.randint(0, SCREEN_HEIGHT),
            )
        )

    # Move the cloud based on a constant, randomly-generated, speed
    # Remove the Sprite when the cloud leaves the left edge of the screen
    def update(self):
        self.rect.move_ip(-5, 0)
        if self.rect.right < 0:
            self.kill()


# Create the screen object
# The size is determined by the constants SCREEN_WIDTH and SCREEN_HEIGHT
screen = pg.display.set_mode((SCREEN_WIDTH, SCREEN_HEIGHT))

# Create custom events for adding a new enemy and new clouds
# This line creates a unique event ID for the ADD_ENEMY event,
# USEREVENT is the last registered event in the list pygame generates
# so USEREVENT + 1 is the next available
ADD_ENEMY = pg.USEREVENT + 1
pg.time.set_timer(ADD_ENEMY, 250)

ADD_CLOUD = pg.USEREVENT + 2
pg.time.set_timer(ADD_CLOUD, 1000)


# Instantiate the player, currently just a rectangle
player = Player()

# Create groups to hold enemy sprites and all sprites
# - enemies is used for collision detection and position updates
# - all_sprites is used for rendering
enemies = pg.sprite.Group()
clouds = pg.sprite.Group()
all_sprites = pg.sprite.Group()
all_sprites.add(player)

# Create a variable to keep the game running
running = True

# Setup the clock for a decent frame rate
clock = pg.time.Clock()

# Load and play background music
# Music source: http://ccmixter.org/files/Apoxode/59262
# License: https://creativecommons.org/licenses/by/3.0/
pg.mixer.music.load("bg-music.mp3")
pg.mixer.music.play(loops=-1)

# Load all sound files for sound effects
# Sound sources: Jon Fincher
move_up_sound = pg.mixer.Sound("rising_putter.ogg")
move_down_sound = pg.mixer.Sound("falling_putter.ogg")
collision_sound = pg.mixer.Sound("collision.ogg")

# Main loop
while running:
    # First, we need to handle any events that have occurred since the last loop
    # Start by looking at every event in the queue and matching their type
    for event in pg.event.get():
        # was the event a user key press?
        if event.type == KEYDOWN:
            # was it the esc key?
            if event.key == K_ESCAPE:
                # end the game loop
                running = False

        # Is the event an add enemy event?
        elif event.type == ADD_ENEMY:
            # Create the new enemy and add it to sprite groups
            new_enemy = Enemy()
            enemies.add(new_enemy)
            all_sprites.add(new_enemy)

        # Is the event an add cloud event?
        elif event.type == ADD_CLOUD:
            # Create a new cloud and add it to the clouds and all_sprites groups
            new_cloud = Cloud()
            clouds.add(new_cloud)
            all_sprites.add(new_cloud)

        # else, was the event a window close?
        elif event.type == QUIT:
            # end the game loop
            running = False

    # Get the set of keys pressed and check for user input
    key_presses = pg.key.get_pressed()

    # Update the player sprite based on user key presses
    player.update(key_presses)

    # Update enemy positions
    enemies.update()
    clouds.update()

    # Fill the screen in sky blue
    screen.fill((135, 206, 250))

# Create a surface and pass in a tuple containing its length and width
#   surf = pg.Surface((50, 50))
#
# Give the surface a color to separate it from the background
#   surf.fill((0, 0, 0))
#   rect = surf.get_rect()

# This line draws the newly created surf Surface onto the screen Surface
# at the center axis of the screen
#   screen.blit(surf, (SCREEN_WIDTH/2, SCREEN_HEIGHT/2))
# Actually that will put the top left corner of the surf object at the
# exact center. To line up the centers, some math needs to be done:
#   surf_center = (
#       (SCREEN_WIDTH - surf.get_width()) / 2,
#       (SCREEN_HEIGHT - surf.get_height()) / 2
#   )
#
#   screen.blit(surf, surf_center)
#   pg.display.flip()

    # Draw the player on the screen
    # Passing the player Rect to blit, it uses the top left corner to draw the surface
    #   screen.blit(player.surf, player.rect)
    # Once using Sprite Groups, you can iterate over all sprites and blit them in a loop
    for entity in all_sprites:
        screen.blit(entity.surf, entity.rect)

    # Check if any enemies have collided with the player
    if pg.sprite.spritecollideany(player, enemies):
        # If yes, then player has failed, stop game loop
        player.kill()

        # Stop any movement sounds and play the collision sound
        move_up_sound.stop()
        move_down_sound.stop()
        collision_sound.play()

        # Stop the game loop
        running = False

    # Update the display
    pg.display.flip()

    # Ensure program maintains a rate of 30 frames per second
    clock.tick(30)

# All done, stop and quit the sound mixer
pg.mixer.music.stop()
pg.mixer.quit()
