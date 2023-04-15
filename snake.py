# Here is a basic implementation of a snake game using Pygame library

# Importing necessary libraries
import pygame
import time
import random

# Initializing Pygame
pygame.init()

# Defining colors
white = (255, 255, 255)
black = (0, 0, 0)
red = (213, 50, 80)
green = (0, 255, 0)
blue = (50, 153, 213)

# Setting the width and height of the screen
width = 600
height = 400

# Creating the screen
screen = pygame.display.set_mode((width, height))

# Setting the title of the screen
pygame.display.set_caption("Snake Game")

# Setting the font style and size
font_style = pygame.font.SysFont(None, 30)

# Defining a function to display the message on the screen
def message(msg, color):
    mesg = font_style.render(msg, True, color)
    screen.blit(mesg, [width/6, height/3])

# Defining the speed of the snake
snake_speed = 5

# Defining the size of the block
block_size = 10

# Defining the function to display the snake on the screen
def our_snake(block_size, snake_list):
    for x in snake_list:
        pygame.draw.rect(screen, black, [x[0], x[1], block_size, block_size])

# Defining the main function
def gameLoop():
    game_over = False
    game_close = False

    # Starting position of the snake
    x1 = width / 2
    y1 = height / 2

    # Changing the position of the snake
    x1_change = 0       
    y1_change = 0

    # Creating the snake
    snake_List = []
    Length_of_snake = 1

    # Generating the food for the snake
    foodx = round(random.randrange(0, width - block_size) / 10.0) * 10.0
    foody = round(random.randrange(0, height - block_size) / 10.0) * 10.0

    # Running the game
    while not game_over:

        # Displaying the message when the game is over
        while game_close == True:
            screen.fill(blue)
            message("You Lost! Press Q-Quit or C-Play Again", red)
            pygame.display.update()

            # Checking for the user input
            for event in pygame.event.get():
                if event.type == pygame.KEYDOWN:
                    if event.key == pygame.K_q:
                        game_over = True
                        game_close = False
                    if event.key == pygame.K_c:
                        gameLoop()

        # Changing the position of the snake
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                game_over = True
            if event.type == pygame.KEYDOWN:
                if event.key == pygame.K_LEFT:
                    x1_change = -block_size
                    y1_change = 0
                elif event.key == pygame.K_RIGHT:
                    x1_change = block_size
                    y1_change = 0
                elif event.key == pygame.K_UP:
                    y1_change = -block_size
                    x1_change = 0
                elif event.key == pygame.K_DOWN:
                    y1_change = block_size
                    x1_change = 0

        # Checking if the snake hits the boundary
        if x1 >= width or x1 < 0 or y1 >= height or y1 < 0:
            game_close = True

        # Changing the position of the snake
        x1 += x1_change
        y1 += y1_change

        # Creating the background color
        screen.fill(blue)

        # Creating the food for the snake
        pygame.draw.rect(screen, green, [foodx, foody, block_size, block_size])

        # Creating the snake
        snake_Head = []
        snake_Head.append(x1)
        snake_Head.append(y1)
        snake_List.append(snake_Head)
        if len(snake_List) > Length_of_snake:
            del snake_List[0]

        # Checking if the snake hits itself
        for x in snake_List[:-1]:
            if x == snake_Head:
                game_close = True

        # Displaying the snake on the screen
        our_snake(block_size, snake_List)

        # Updating the screen
        pygame.display.update()

        # Checking if the snake hits the food
        if x1 == foodx and y1 == foody:
            foodx = round(random.randrange(0, width - block_size) / 10.0) * 10.0
            foody = round(random.randrange(0, height - block_size) / 10.0) * 10.0
            Length_of_snake += 1

        # Setting the speed of the snake
        clock = pygame.time.Clock()
        clock.tick(snake_speed)

    # Quitting Pygame
    pygame.quit()

    # Quitting Python
    quit()

# Running the game
gameLoop()