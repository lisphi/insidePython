import pygame

class Snake:
    def __init__(self):
        self.body = [(SCREEN_WIDTH/2, SCREEN_HEIGHT/2)]
        self.direction = "RIGHT"
        self.color = (0, 255, 0)

    def move(self):
        x, y = self.body[0]
        if self.direction == "UP":
            y -= BLOCK_SIZE
        elif self.direction == "DOWN":
            y += BLOCK_SIZE
        elif self.direction == "LEFT":
            x -= BLOCK_SIZE
        elif self.direction == "RIGHT":
            x += BLOCK_SIZE
        self.body.insert(0, (x, y))
        self.body.pop()

    def grow(self):
        x, y = self.body[-1]
        if self.direction == "UP":
            y += BLOCK_SIZE
        elif self.direction == "DOWN":
            y -= BLOCK_SIZE
        elif self.direction == "LEFT":
            x += BLOCK_SIZE
        elif self.direction == "RIGHT":
            x -= BLOCK_SIZE
        self.body.append((x, y))

    def collides_with(self, food):
        return self.body[0] == food.position

    def turn_up(self):
        if self.direction != "DOWN":
            self.direction = "UP"

    def turn_down(self):
        if self.direction != "UP":
            self.direction = "DOWN"

    def turn_left(self):
        if self.direction != "RIGHT":
            self.direction = "LEFT"

    def turn_right(self):
        if self.direction != "LEFT":
            self.direction = "RIGHT"

    def draw(self, surface):
        for block in self.body:
            rect = pygame.Rect(block[0], block[1], BLOCK_SIZE, BLOCK_SIZE)
            pygame.draw.rect(surface, self.color, rect)

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
