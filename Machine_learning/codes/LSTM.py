import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.optimizers import Adam
import matplotlib.pyplot as plt

# Load the data
df = pd.read_csv('table_tennis_matches.csv')

# Display columns to understand the data structure
print(df.columns.tolist())

def preprocess_data(df):
    # Convert timestamp to datetime
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    
    # Sort by match ID and timestamp
    df = df.sort_values(['Match ID', 'Timestamp'])
    
    # Fill N/A values appropriately
    df.fillna(0, inplace=True)
    
    # Create target variables
    df['home_won'] = (df['winner_home'] > 0.5).astype(int)
    df['away_won'] = (df['winner_away'] > 0.5).astype(int)
    
    # Calculate arbitrage opportunities (simplified example)
    df['arbitrage_opportunity'] = 0
    for match_id in df['Match ID'].unique():
        match_data = df[df['Match ID'] == match_id]
        if len(match_data) > 1:
            # Check if odds changed enough to create arbitrage
            home_odds_change = match_data['winner_home'].max() - match_data['winner_home'].min()
            away_odds_change = match_data['winner_away'].max() - match_data['winner_away'].min()
            if home_odds_change > 0.2 or away_odds_change > 0.2:  # Threshold
                df.loc[df['Match ID'] == match_id, 'arbitrage_opportunity'] = 1
    
    # Select features - adjust based on your needs
    features = [
        'match_score_home', 'match_score_away',
        'set_1_home_point', 'set_1_away_point',
        'set_2_home_point', 'set_2_away_point',
        'set_3_home_point', 'set_3_away_point',
        '1st_game_-_winner_home', '1st_game_-_winner_away',
        '1st_game_-_1st_point_home', '1st_game_-_1st_point_away',
        'total_points_over_73.5', 'total_points_under_73.5',
        'winner_home', 'winner_away'  # Current probabilities
    ]
    
    # Create sequences for LSTM
    sequence_length = 5  # Number of past records to consider
    X, y_winner, y_arbitrage = [], [], []
    
    for match_id in df['Match ID'].unique():
        match_data = df[df['Match ID'] == match_id][features]
        if len(match_data) >= sequence_length:
            for i in range(len(match_data) - sequence_length):
                X.append(match_data.iloc[i:i+sequence_length].values)
                # Use the final outcome as label
                y_winner.append(df[df['Match ID'] == match_id]['home_won'].iloc[-1])
                y_arbitrage.append(df[df['Match ID'] == match_id]['arbitrage_opportunity'].iloc[-1])
    
    X = np.array(X)
    y_winner = np.array(y_winner)
    y_arbitrage = np.array(y_arbitrage)
    
    return X, y_winner, y_arbitrage

X, y_winner, y_arbitrage = preprocess_data(df)


def build_winner_model(input_shape):
    model = Sequential([
        LSTM(64, return_sequences=True, input_shape=input_shape),
        Dropout(0.2),
        LSTM(32),
        Dropout(0.2),
        Dense(16, activation='relu'),
        Dense(1, activation='sigmoid')
    ])
    
    model.compile(optimizer=Adam(learning_rate=0.001),
                 loss='binary_crossentropy',
                 metrics=['accuracy'])
    
    return model

# Split data
X_train, X_test, y_train, y_test = train_test_split(X, y_winner, test_size=0.2, random_state=42)

# Normalize data
scaler = MinMaxScaler()
X_train = scaler.fit_transform(X_train.reshape(-1, X_train.shape[-1])).reshape(X_train.shape)
X_test = scaler.transform(X_test.reshape(-1, X_test.shape[-1])).reshape(X_test.shape)

# Build and train model
winner_model = build_winner_model((X_train.shape[1], X_train.shape[2]))
history = winner_model.fit(X_train, y_train, 
                          epochs=50, 
                          batch_size=32, 
                          validation_split=0.1,
                          verbose=1)

# Evaluate
test_loss, test_acc = winner_model.evaluate(X_test, y_test)
print(f"Test Accuracy: {test_acc:.4f}")

# Plot training history
plt.plot(history.history['accuracy'], label='Train Accuracy')
plt.plot(history.history['val_accuracy'], label='Validation Accuracy')
plt.title('Model Accuracy')
plt.ylabel('Accuracy')
plt.xlabel('Epoch')
plt.legend()
plt.show()


def build_arbitrage_model(input_shape):
    model = Sequential([
        LSTM(64, return_sequences=True, input_shape=input_shape),
        Dropout(0.2),
        LSTM(32),
        Dropout(0.2),
        Dense(16, activation='relu'),
        Dense(1, activation='sigmoid')
    ])
    
    model.compile(optimizer=Adam(learning_rate=0.001),
                 loss='binary_crossentropy',
                 metrics=['accuracy'])
    
    return model

# Split data for arbitrage model
X_train_arb, X_test_arb, y_train_arb, y_test_arb = train_test_split(
    X, y_arbitrage, test_size=0.2, random_state=42
)

# Normalize (using same scaler as before)
X_train_arb = scaler.transform(X_train_arb.reshape(-1, X_train_arb.shape[-1])).reshape(X_train_arb.shape)
X_test_arb = scaler.transform(X_test_arb.reshape(-1, X_test_arb.shape[-1])).reshape(X_test_arb.shape)

# Build and train arbitrage model
arbitrage_model = build_arbitrage_model((X_train_arb.shape[1], X_train_arb.shape[2]))
history_arb = arbitrage_model.fit(X_train_arb, y_train_arb, 
                                epochs=50, 
                                batch_size=32, 
                                validation_split=0.1,
                                verbose=1)

# Evaluate
test_loss_arb, test_acc_arb = arbitrage_model.evaluate(X_test_arb, y_test_arb)
print(f"Arbitrage Test Accuracy: {test_acc_arb:.4f}")


def predict_and_strategize(model, arbitrage_model, match_data, initial_bankroll=1000):
    # Preprocess the match data
    match_data_processed = scaler.transform(match_data.reshape(-1, match_data.shape[-1])).reshape(match_data.shape)
    
    # Predict winner probability
    winner_prob = model.predict(match_data_processed[-1:])[0][0]
    home_win_prob = winner_prob
    away_win_prob = 1 - winner_prob
    
    # Predict arbitrage opportunity
    arb_prob = arbitrage_model.predict(match_data_processed[-1:])[0][0]
    
    # Determine betting strategy
    strategy = {
        'home_win_prob': home_win_prob,
        'away_win_prob': away_win_prob,
        'arbitrage_opportunity': arb_prob > 0.5,
        'arbitrage_confidence': arb_prob,
        'recommended_bet': None,
        'stake': 0
    }
    
    if arb_prob > 0.7:  # High confidence in arbitrage
        # Check which player's odds are more likely to increase
        odds_movement = np.diff(match_data[:, -2:], axis=0)  # Track winner_home, winner_away columns
        
        if np.mean(odds_movement[:, 0]) > np.mean(odds_movement[:, 1]):
            # Home odds increasing faster - bet away first
            strategy['recommended_bet'] = 'away'
            strategy['stake'] = min(initial_bankroll * 0.1, 100)  # 10% or max 100
        else:
            # Away odds increasing faster - bet home first
            strategy['recommended_bet'] = 'home'
            strategy['stake'] = min(initial_bankroll * 0.1, 100)
    elif home_win_prob > 0.6:
        strategy['recommended_bet'] = 'home'
        strategy['stake'] = min(initial_bankroll * 0.05, 50)  # 5% or max 50
    elif away_win_prob > 0.6:
        strategy['recommended_bet'] = 'away'
        strategy['stake'] = min(initial_bankroll * 0.05, 50)
    else:
        strategy['recommended_bet'] = 'no bet'
    
    return strategy

# Example usage
sample_match = X_test[0:1]  # Take first test match
strategy = predict_and_strategize(winner_model, arbitrage_model, sample_match)
print("Betting Strategy:")
print(strategy)