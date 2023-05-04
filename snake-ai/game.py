class Game:
    def __init__(self):
        self.snake = Snake()
        self.food = Food()
        self.score = 0

    def update(self):
        self.snake.move()
        if self.snake.collides_with(self.food):
            self.snake.grow()
            self.food.randomize_position()
            self.score += 1

    def draw(self, surface):
        self.snake.draw(surface)
        self.food.draw(surface)

    def handle_events(self, events):
        for event in events:
            if event.type == pygame.QUIT:
                pygame.quit()
                sys.exit()

            if event.type == pygame.KEYDOWN:
                if event.key == pygame.K_UP:
                    self.snake.turn_up()
                elif event.key == pygame.K_DOWN:
                    self.snake.turn_down()
                elif event.key == pygame.K_LEFT:
                    self.snake.turn_left()
                elif event.key == pygame.K_RIGHT:
                    self.snake.turn_right()

    def run(self):
        pygame.init()
        clock = pygame.time.Clock()
        screen = pygame.display.set_mode((SCREEN_WIDTH, SCREEN_HEIGHT))
        pygame.display.set_caption("Snake Game")

        while True:
            clock.tick(10)
            self.handle_events(pygame.event.get())
            self.update()
            screen.fill((0, 0, 0))
            self.draw(screen)
            pygame.display.update()
