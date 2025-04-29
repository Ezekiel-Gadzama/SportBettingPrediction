import numpy as np
from scipy.optimize import fsolve

class OddsCalculator:
    def __init__(self, draw_odds, draw_stakes, current_draw_odd, away_odds, away_stakes, current_away_odd):
        self.draw_odds = draw_odds
        self.draw_stakes = draw_stakes
        self.current_draw_odd = current_draw_odd
        self.away_odds = away_odds
        self.away_stakes = away_stakes
        self.current_away_odd = current_away_odd
    
    def equations(self, variables):
        stake2, stake3 = variables
        left_odd_numerator = sum(odd * stake for odd, stake in zip(self.draw_odds, self.draw_stakes)) + self.current_draw_odd
        left_odd_denominator = sum(self.draw_stakes) + stake2
        left_odd = left_odd_numerator / left_odd_denominator
        
        right_odd_numerator = sum(odd * stake for odd, stake in zip(self.away_odds, self.away_stakes)) + self.current_away_odd
        right_odd_denominator = sum(self.away_stakes) + stake3
        right_odd = right_odd_numerator / right_odd_denominator
        
        eq1 = stake2 * left_odd - stake3 * right_odd
        return [eq1, eq1]  # We return the same equation twice to match the number of variables
    
    def calculate_stakes(self):
        # Initial guess for stake2 and stake3
        initial_guess = [1.0, 1.0]
        # Solve the system of equations
        solution = fsolve(self.equations, initial_guess)
        stake2, stake3 = solution
        return stake2, stake3

# Example usage:
draw_odds = [2.0, 3.0]
draw_stakes = [100, 150]
current_draw_odd = 2.5
away_odds = [1.8, 2.2]
away_stakes = [200, 180]
current_away_odd = 2.0

calculator = OddsCalculator(draw_odds, draw_stakes, current_draw_odd, away_odds, away_stakes, current_away_odd)
stake2, stake3 = calculator.calculate_stakes()
print(f"stake2 = {stake2}, stake3 = {stake3}")