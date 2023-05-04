import random
import pygame

from snake import BLOCK_SIZE, SCREEN_WIDTH, SCREEN_HEIGHT

class Food:
    def __init__(self):
        self.position = (0, 0)
        self.color = (255, 0, 0)
        self.randomize_position()

    def randomize_position(self):
        x = random.randint(0, (SCREEN_WIDTH-BLOCK_SIZE)//BLOCK_SIZE) * BLOCK_SIZE
        y = random.randint(0, (SCREEN_HEIGHT-BLOCK_SIZE)//BLOCK_SIZE) * BLOCK_SIZE
        self.position = (x, y)

    def draw(self, surface):
        rect = pygame.Rect(self.position[0], self.position[1], BLOCK_SIZE, BLOCK_SIZE)
        pygame.draw.rect(surface, self.color, rect)
