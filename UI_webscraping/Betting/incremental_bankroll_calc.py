"""
Utility: incremental staking bankroll calculator + multi-thread round simulation.

Math matches LiveBettingBot incremental mode:
- total_exposure_factor / stake_schedule (same as _inc_total_exposure_factor / _inc_next_stake_amount)

Simulation (edit constants in main()):
- num_threads: T threads; each round EVERY thread uses the SAME stake S (same odd).
- wins_per_round: exactly W wins (T - W lose). Losers are threads 0..F-1.
- Per-thread share of loss_pool: share = loss_pool / T. Each thread's stake must be able to
  cover that share if it wins: need profit S*(o-1) >= share  =>  S >= share/(o-1).
- S each round = max(min_base, incremental_stake(shared_trial), share/(o-1)), where
  incremental_stake uses stake_schedule(avg_odd, trials, min_base) at index shared_trial.
- shared_trial starts at 1; if any thread loses this round (F>0), shared_trial += 1 (cap at trials);
  if everyone wins (F==0), shared_trial resets to 1.
- split_loss: per loser, add split_loss * min_base * (o-1) to loss_pool (forgone trial-1 profit).
- Win profits repay loss_pool (accounting only; cash already in balance).

Stops when balance <= 0, cannot afford T*S, or max_rounds.
"""

from __future__ import annotations


def total_exposure_factor(avg_odd: float, trials: int) -> float:
    o = float(avg_odd)
    n = max(1, int(trials))
    if o <= 1.0:
        return float("inf")
    profit_unit = o - 1.0
    spent = 0.0
    for k in range(1, n + 1):
        if k == 1:
            stake_k = 1.0
        else:
            stake_k = (spent + profit_unit * k) / (o - 1.0)
        spent += stake_k
    return float(spent)


def stake_schedule(avg_odd: float, trials: int, base_stake: float) -> list[float]:
    o = float(avg_odd)
    n = max(1, int(trials))
    base = float(base_stake)
    if o <= 1.0:
        raise ValueError("avg_odd must be > 1.0")
    profit_unit = base * (o - 1.0)
    spent = 0.0
    out: list[float] = []
    for k in range(1, n + 1):
        if k == 1:
            stake_k = base
        else:
            stake_k = (spent + profit_unit * k) / (o - 1.0)
        out.append(float(stake_k))
        spent += float(stake_k)
    return out


def required_bankroll(avg_odd: float, trials: int, min_base_stake: float = 10.0) -> float:
    factor = total_exposure_factor(avg_odd, trials)
    if factor == float("inf"):
        return float("inf")
    return float(min_base_stake) * float(factor)


def stake_for_trial(sched: list[float], trial_index: int) -> float:
    """trial_index is 1-based (trial 1 = first stake)."""
    k = max(1, min(trial_index, len(sched))) - 1
    return float(sched[k])


def simulate_rounds_until_ruin(
    *,
    avg_odd: float,
    max_trials: int,
    min_base: float,
    initial_bankroll: float,
    num_threads: int,
    wins_per_round: int,
    split_loss: float,
    max_rounds: int = 100_000,
    verbose: bool = False,
) -> tuple[int, float, str, list[tuple[int, float, float]]]:
    """
    Returns (rounds_completed, final_balance, stop_reason, history).

    stop_reason: "bankroll_depleted" | "cannot_afford_next_round" | "max_rounds"

    Deterministic: lowest-index threads lose each round (indices 0..F-1).
    """
    o = float(avg_odd)
    if o <= 1.0:
        raise ValueError("avg_odd must be > 1.0")
    T = max(1, int(num_threads))
    W = max(0, min(int(wins_per_round), T))
    F = T - W
    split_loss = max(0.0, float(split_loss))

    trials_cap = max(1, int(max_trials))
    balance = float(initial_bankroll)
    loss_pool = 0.0
    # One shared trial index for all threads (1..trials_cap).
    shared_trial = 1
    sched_min = stake_schedule(o, trials_cap, float(min_base))
    history: list[tuple[int, float, float]] = []

    for r in range(1, max_rounds + 1):
        share = loss_pool / T if T > 0 else 0.0
        S_recover = share / (o - 1.0) if share > 1e-12 else 0.0
        S_inc = stake_for_trial(sched_min, shared_trial)
        S = max(float(min_base), float(S_inc), float(S_recover))
        S = round(S, 2)

        stakes = [S] * T
        total_stake = S * T
        if balance < total_stake - 1e-9:
            if verbose:
                print(f"\n  --- Round {r} (cannot start) ---")
                print(f"  balance={balance:.2f}  need total_stake={total_stake:.2f}")
            return r - 1, balance, "cannot_afford_next_round", history

        lose_set = set(range(F))
        win_set = set(range(T)) - lose_set

        if verbose:
            print(f"\n  ========== Round {r} ==========")
            print(f"  balance BEFORE round: {balance:.2f}")
            print(
                f"  equal stake S={S:.2f}  (shared_trial={shared_trial}, "
                f"loss_pool={loss_pool:.2f} -> share/thread={share:.2f}, "
                f"S_recover={S_recover:.2f}, S_inc={S_inc:.2f}, min_base={min_base:.2f})"
            )
            for i in range(T):
                print(f"    thread {i}: stake={stakes[i]:.2f}")

        # --- Cash flow (same as book: pay all stakes, then winners get stake*odd back) ---
        balance_before = balance
        balance -= total_stake
        if verbose:
            stake_sum_str = " + ".join(f"{s:.2f}" for s in stakes)
            print(f"  Pay all stakes: -{total_stake:.2f}  ({stake_sum_str} = {total_stake:.2f})")
            print(f"  balance after deducting ALL stakes: {balance:.2f}  (= {balance_before:.2f} - {total_stake:.2f})")

        total_return_to_winners = 0.0
        win_profit = 0.0
        for i in sorted(win_set):
            stake_i = stakes[i]
            ret = stake_i * o  # full return = money back + profit
            profit_i = stake_i * (o - 1.0)
            total_return_to_winners += ret
            win_profit += profit_i
            if verbose:
                print(
                    f"    thread {i} WINS: credit stake*odd = {stake_i:.2f} * {o:.4f} = {ret:.2f} "
                    f"(profit +{profit_i:.2f})"
                )

        balance += total_return_to_winners
        if verbose:
            print(f"  Total credited to winners: +{total_return_to_winners:.2f}")
            for i in sorted(lose_set):
                print(
                    f"    thread {i} LOSES: stake {stakes[i]:.2f} already left balance; "
                    f"no return (net -{stakes[i]:.2f} vs start of round)."
                )
            print(f"  balance AFTER paying winners: {balance:.2f}")
            print(
                f"  Net change this round (cash only): {balance - balance_before:+.2f} "
                f"(= -losers' stakes + winners' profits)"
            )

        # Loss pool — separate from the balance ledger above
        cash_lost_by_losers = sum(stakes[i] for i in lose_set)
        opportunity = split_loss * F * float(min_base) * (o - 1.0)
        loss_pool += cash_lost_by_losers + opportunity

        if F > 0:
            shared_trial = min(trials_cap, shared_trial + 1)
        else:
            shared_trial = 1

        # Repay loss pool from win profits (accounting; does not move balance again)
        rep = min(loss_pool, win_profit)
        loss_pool -= rep
        if verbose:
            print(
                f"  loss_pool: +{cash_lost_by_losers + opportunity:.2f} "
                f"(cash_lost {cash_lost_by_losers:.2f} + opportunity {opportunity:.2f}), "
                f"then repaid from win profit -{rep:.2f} -> loss_pool now {loss_pool:.2f}"
            )
            print(f"  next round shared_trial will be {shared_trial}")

        history.append((r, balance, loss_pool))

        if balance <= 0:
            return r, balance, "bankroll_depleted", history

    return max_rounds, balance, "max_rounds", history


def main() -> None:
    # --- Single-thread bankroll (same as before) ---
    avg = 1.17
    trials = 3
    min_base = 100.0

    factor = total_exposure_factor(avg, trials)
    need = required_bankroll(avg, trials, min_base_stake=min_base)

    print(f"avg_odd={avg:.4f} trials={trials} min_base={min_base:.2f}")
    print(f"exposure_factor(base=1)={factor:.6f}")
    if need == float("inf"):
        print("bankroll_needed=inf (avg_odd must be > 1.0)")
    else:
        print(f"bankroll_needed_for_min_base={need:.2f}  (= min_base * factor)")

    sched = stake_schedule(avg, trials, min_base)
    print("trial_stakes (base=min_base):")
    for i, s in enumerate(sched, start=1):
        print(f"  trial {i}: {s:.2f}")
    print(f"total_exposure={sum(sched):.2f}")

    # --- Multi-thread round simulation (edit these) ---
    num_threads = 4
    wins_per_round = 3
    split_loss = 1.0
    max_rounds_cap = 50_000

    print()
    print("--- Round simulation (until bankroll exhausted or max_rounds) ---")
    print(f" num_threads={num_threads} wins_per_round={wins_per_round} fails_per_round={num_threads - wins_per_round}")
    print(f"  split_loss={split_loss}  (opportunity cost weight per loser)")
    print(f"  initial_bankroll={need:.2f} (from bankroll_needed above)")

    rounds, final_bal, reason, hist = simulate_rounds_until_ruin(
        avg_odd=avg,
        max_trials=trials,
        min_base=min_base,
        initial_bankroll=need,
        num_threads=num_threads,
        wins_per_round=wins_per_round,
        split_loss=split_loss,
        max_rounds=max_rounds_cap,
        verbose=True,
    )

    print(f"  rounds_completed={rounds} final_balance={final_bal:.2f} stop={reason}")
    if len(hist) <= 20:
        for rr, b, lp in hist:
            print(f"    round {rr}: balance={b:.2f} loss_pool={lp:.2f}")
    else:
        for rr, b, lp in hist[:5]:
            print(f"    round {rr}: balance={b:.2f} loss_pool={lp:.2f}")
        print(f"    ... ({len(hist) - 10} rounds omitted) ...")
        for rr, b, lp in hist[-5:]:
            print(f"    round {rr}: balance={b:.2f} loss_pool={lp:.2f}")


if __name__ == "__main__":
    main()
