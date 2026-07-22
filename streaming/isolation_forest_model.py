import pickle
import random

FEATURES = ["motor_temp", "motor_current", "battery_pct", "v_lin", "min_obstacle_dist"]

def generate_nominal_samples(health_cfg, n=5000, seed=42):
    rng = random.Random(seed)
    temp_cfg = health_cfg["motor_temp"]
    current_cfg = health_cfg["motor_current"]

    samples = []
    for _ in range(n):
        motor_temp = rng.gauss(temp_cfg["nominal_c"], temp_cfg["noise_std_c"])
        motor_current = rng.gauss(current_cfg["nominal_a"], current_cfg["noise_std_a"])
        battery_pct = rng.uniform(15.0, 100.0)

        moving = rng.random() >= 0.4
        if moving:
            v_lin = abs(rng.gauss(0.20, 0.03))
            r = rng.random()
            if r < 0.7:
                min_obstacle_dist = min(3.5, abs(rng.gauss(3.4, 0.2)))
            elif r < 0.9:
                min_obstacle_dist = rng.uniform(0.8, 1.5)
            else:
                min_obstacle_dist = rng.uniform(0.3, 3.5)
        else:
            v_lin = abs(rng.gauss(0.0, 0.01))
            r = rng.random()
            if r < 0.5:
                min_obstacle_dist = min(3.5, abs(rng.gauss(3.4, 0.2)))
            else:
                min_obstacle_dist = rng.uniform(0.3, 2.0)

        samples.append([motor_temp, motor_current, battery_pct, v_lin, min_obstacle_dist])
    return samples

def train_model(health_cfg, n=5000, seed=42):
    from sklearn.ensemble import IsolationForest

    samples = generate_nominal_samples(health_cfg, n=n, seed=seed)
    model = IsolationForest(n_estimators=100, contamination=0.02, random_state=seed)
    model.fit(samples)
    return model

def save_model(model, path):
    with open(path, "wb") as f:
        pickle.dump(model, f)

def load_or_train_model(path, health_cfg):
    try:
        with open(path, "rb") as f:
            return pickle.load(f)
    except FileNotFoundError:
        model = train_model(health_cfg)
        try:
            save_model(model, path)
        except OSError:
            pass
        return model
