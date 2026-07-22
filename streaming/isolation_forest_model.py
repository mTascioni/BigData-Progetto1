"""Modello Isolation Forest per la detection delle anomalie di salute. Il
modello viene allenato su un campione sintetico di telemetria "nominale",
generato a partire dagli stessi parametri (health_channels_nominal)
fissati in config/experiment.json -- stessa fonte di verita' usata dal
nodo-ponte per sintetizzare i canali di salute. Puo' essere ri-allenato
sullo storico reale una volta accumulato, sostituendo
generate_nominal_samples() con una lettura da Parquet.
"""
import pickle
import random

FEATURES = ["motor_temp", "motor_current", "battery_pct", "v_lin", "min_obstacle_dist"]


def generate_nominal_samples(health_cfg, n=5000, seed=42):
    """Campiona n vettori di feature plausibili per telemetria SENZA guasti,
    nello stesso ordine di FEATURES."""
    rng = random.Random(seed)
    temp_cfg = health_cfg["motor_temp"]
    current_cfg = health_cfg["motor_current"]

    samples = []
    for _ in range(n):
        motor_temp = rng.gauss(temp_cfg["nominal_c"], temp_cfg["noise_std_c"])
        motor_current = rng.gauss(current_cfg["nominal_a"], current_cfg["noise_std_a"])
        battery_pct = rng.uniform(15.0, 100.0)  # un robot nominale opera su tutto il range di carica

        # v_lin e min_obstacle_dist NON sono indipendenti nella realta' (Passo
        # 14, trovato con dati reali): un robot fermo resta parcheggiato
        # dov'e' -- se quel punto e' vicino a una parete/dock/altro robot, il
        # lidar legge distanza corta per TUTTA la durata della sosta, anche
        # a lungo, non come un evento raro e transitorio. Il campionamento
        # indipendente originale sotto-rappresentava fortemente questa
        # combinazione (idle + vicino), causando falsi positivi sistematici
        # e persistenti dell'Isolation Forest su robot fermi in un punto
        # qualsiasi vicino a qualcosa (osservato: R3 idle, min_obstacle_dist
        # ~0.45m per oltre 30 tick consecutivi, temp/corrente nominali).
        moving = rng.random() >= 0.4
        if moving:
            v_lin = abs(rng.gauss(0.20, 0.03))  # in movimento, burger ~0.15-0.22 m/s
            r = rng.random()
            if r < 0.7:
                min_obstacle_dist = min(3.5, abs(rng.gauss(3.4, 0.2)))  # area aperta, nessun ostacolo vicino
            elif r < 0.9:
                min_obstacle_dist = rng.uniform(0.8, 1.5)  # corridoio stretto: pareti vicine, normale
            else:
                min_obstacle_dist = rng.uniform(0.3, 3.5)  # incrocia un altro robot/scaffale
        else:
            v_lin = abs(rng.gauss(0.0, 0.01))  # fermo/idle
            r = rng.random()
            if r < 0.5:
                min_obstacle_dist = min(3.5, abs(rng.gauss(3.4, 0.2)))  # fermo in un'area aperta
            else:
                min_obstacle_dist = rng.uniform(0.3, 2.0)  # parcheggiato vicino a qualcosa: stato stabile, non raro

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
    """Carica il modello da `path` se esiste, altrimenti lo allena al volo
    (fallback robusto: il job di detection non deve mai bloccarsi per un
    pickle mancante) e lo salva per i run successivi."""
    try:
        with open(path, "rb") as f:
            return pickle.load(f)
    except FileNotFoundError:
        model = train_model(health_cfg)
        try:
            save_model(model, path)
        except OSError:
            pass  # es. filesystem read-only: va bene, si ri-allena al prossimo avvio
        return model
