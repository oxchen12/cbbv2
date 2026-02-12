"""
This module provides utility modules and functions related
to substitution information.
"""
from typing import (
    Callable,
    TypeVar,
    Sequence,
)

from numba import guvectorize, int64
import numpy as np
import polars as pl
import polars.selectors as cs

SUB_ID = 584
PolarsFrame = TypeVar('PolarsFrame', pl.DataFrame, pl.LazyFrame)
SortingBy = Sequence[tuple[str | pl.Expr, bool]]


def sort_by(
    frame: PolarsFrame,
    sorting_by: SortingBy
) -> PolarsFrame:
    cols, descending = zip(*sorting_by)
    return (
        frame
        .sort(
            *cols,
            descending=list(descending)
        )
    )

def sort_plays(
    frame: PolarsFrame,
    with_datetime: bool = False
) -> PolarsFrame:
    sorting_by = [
        ('game_id', False),
        ('period', False),
        ('clock_minutes', True),
        ('clock_seconds', True),
        ('home_score', False),
        ('away_score', False),
        # ('team_id', False),
        ('sequence_id', False)
    ]
    if with_datetime:
        sorting_by.insert(0, ('datetime', False))
    return (
        frame
        .pipe(
            sort_by,
            sorting_by
        )
    )


def period_to_min_mens(period: pl.Expr) -> pl.Expr:
    """
    Period to minutes conversion for men's college basketball.
    """
    return (
        pl.when(period.le(2))
        .then(pl.lit(20))
        .otherwise(pl.lit(5))
    )


def period_list_to_min_mens(periods: pl.Expr) -> pl.Expr:
    """
    List-vectorized period to minutes conversion for
    men's college basketball.
    """
    return (
        periods
        .list.eval(pl.element().pipe(period_to_min_mens))
    )


def add_sub_bookends(
    plays: PolarsFrame,
    game_logs: PolarsFrame,
    period_to_min: Callable[[pl.Expr], pl.Expr] = period_to_min_mens,
    keep_non_sub: bool = True
) -> PolarsFrame:
    """
    Adds dummy substitution plays for players starting
    and finishing the game on the court.
    """
    subs = (
        plays
        .filter(
            pl.col('play_type_id').eq(SUB_ID)
        )
    )
    starts = (
        game_logs
        .filter(
            pl.col('started').eq(True),
        )
        .join(
            plays.unique(['game_id', 'player_id']),
            on=['game_id', 'player_id'],
        )
        .select(
            'game_id',
            pl.col('player_id'),
            pl.col('player_id')
            .mul(10)
            .neg()
            .alias('sequence_id'),
            pl.lit(1).alias('period'),
            pl.lit(1).pipe(period_to_min).alias('clock_minutes'),
            pl.lit(0).alias('clock_seconds'),
            'team_id',
            pl.lit(SUB_ID).alias('play_type_id'),
        )
    )
    subs_with_starts = pl.concat(
        [subs, starts],
        how='diagonal_relaxed'
    )

    game_last_period = (
        plays
        .group_by('game_id')
        .agg(pl.max('period'))
    )

    finished_game = (
        subs_with_starts
        .group_by('game_id', 'player_id')
        .having(
            # subbing *in* is always the first action
            # so a player not subbed out by the end of the game
            # must have finished on the court
            pl.len().mod(2).eq(1)
        )
        .agg(
            pl.max('team_id')
        )
        .join(
            game_last_period,
            on='game_id',
        )
    )

    finishes = (
        finished_game
        .select(
            'game_id',
            pl.col('player_id'),
            pl.col('player_id')
            .mul(10)
            .add(1)
            .neg()
            .alias('sequence_id'),
            'period',
            pl.lit(0).alias('clock_minutes'),
            pl.lit(0).alias('clock_seconds'),
            'team_id',
            pl.lit(SUB_ID).alias('play_type_id')
        )
    )

    return (
        pl.concat(
            [plays if keep_non_sub else subs, starts, finishes],
            how='diagonal_relaxed'
        )
        .unique(['game_id', 'sequence_id'])
    )


def with_sub_index(
    plays: PolarsFrame,
    game_logs: PolarsFrame
) -> PolarsFrame:
    plays_with_bookends = (
        plays
        .pipe(add_sub_bookends, game_logs, keep_non_sub=True)
    )

    sub_indices = (
        plays_with_bookends
        .filter(pl.col('play_type_id').eq(SUB_ID))
        .pipe(sort_plays)
        .select(
            'game_id', 'sequence_id',
            pl.col('sequence_id')
            .cum_count()
            .over(['game_id', 'player_id'])
            .sub(1)
            .alias('sub_index')
        )
    )

    return (
        plays_with_bookends
        .join(
            sub_indices,
            on=['game_id', 'sequence_id'],
            how='left'
        )
        .with_columns(pl.col('sub_index').fill_null(-1))
        .pipe(sort_plays)
    )


def with_sub_durations(
    plays: PolarsFrame,
    game_logs: PolarsFrame,
    period_to_min: Callable[[pl.Expr], pl.Expr] = period_list_to_min_mens,
    sub_duration_col: str = 'sub_duration'
) -> PolarsFrame:
    """
    Get substitution durations.
    """

    def player_last_sub(time: pl.Expr) -> pl.Expr:
        return (
            time
            .shift(1)
            .over(['game_id', 'player_id'])
            .name.prefix('last_')
        )

    return (
        plays
        .pipe(with_sub_index, game_logs)
        .filter(~pl.col('sub_index').eq(-1))
        .pipe(sort_plays)
        .with_columns(
            pl.col('period').pipe(player_last_sub),
            pl.col('clock_minutes').pipe(player_last_sub),
            pl.col('clock_seconds').pipe(player_last_sub),
        )
        .with_columns(
            pl.int_ranges(
                pl.col('last_period'),
                pl.col('period')
            )
            .add(1)
            .pipe(period_to_min)
            .list.sum()
            .add(
                pl.col('last_clock_minutes')
                .sub(pl.col('clock_minutes'))
            )
            .add(
                pl.col('last_clock_seconds')
                .sub(pl.col('clock_seconds'))
                .truediv(60)
            )
            .alias(sub_duration_col)
        )
        .select(~cs.starts_with('last_'))
    )


def get_game_minutes(
    plays: PolarsFrame,
    game_logs: PolarsFrame,
    sub_duration_col: str = 'sub_duration'
) -> PolarsFrame:
    """
    Get minutes in play for each player in each game.
    """
    return (
        plays
        .pipe(
            with_sub_durations,
            game_logs,
            sub_duration_col=sub_duration_col
        )
        .filter(pl.col('sub_index').mod(2).eq(1))
        .group_by('game_id', 'player_id')
        .agg(
            pl.sum(sub_duration_col).alias('minutes')
        )
    )


@guvectorize(
    [
        (
                int64[:],
                int64[:],
                int64[:],
                int64[:],
                int64[:],
                int64[:],
                int64[:, :],
                int64[:, :]
        )
    ],
    '(n),(n),(n),(n),(n),(m)->(n,m),(n,m)'
)
def _fill_in_play(
    game_id,
    home_id,
    team_id,
    player_id,
    sub_index,
    dummy_in_play,
    home_in_play,
    away_in_play
):
    """
    Fills in-play information in the provided output arrays.
    Modifies the provided output arrays in-place.
    """
    last_game_id = -1
    last_home_in_play = home_in_play[0]
    last_away_in_play = away_in_play[0]
    for i in range(game_id.shape[0]):
        if last_game_id != game_id[i]:
            last_game_id = game_id[i]
        else:
            for j, _ in enumerate(dummy_in_play):
                home_in_play[i, j] = last_home_in_play[j]
                away_in_play[i, j] = last_away_in_play[j]

        if sub_index[i] < 0:
            continue

        in_play = (
            home_in_play
            if team_id[i] == home_id[i]
            else away_in_play
        )
        # 0 if subbing in
        # 1 if subbing out
        direction = sub_index[i] % 2
        vals = (-1, player_id[i])
        search = vals[direction]
        replace = vals[1 - direction]
        replace_j = -1

        for j, p in enumerate(in_play[i]):
            if p == search:
                replace_j = j
                break

        in_play[i, replace_j] = replace

        if team_id[i] == home_id[i]:
            last_home_in_play = in_play[i]
        else:
            last_away_in_play = in_play[i]


def with_in_play(
    plays: PolarsFrame,
    game_logs: PolarsFrame,
    games: PolarsFrame,
):
    # add home_id
    plays_frame = (
        plays
        .pipe(with_sub_index, game_logs)
        .join(
            games.select('id', 'home_id'),
            left_on='game_id',
            right_on='id'
        )
        .pipe(
            sort_by,
            (
                ('game_id', False),
                ('period', False),
                ('clock_minutes', True),
                ('clock_seconds', True),
                # force bookends to be on ends
                (
                    (
                        pl.when(
                            pl.col('sequence_id').lt(0)
                        )
                        .then(
                            pl.when(pl.col('clock_minutes').gt(0))
                            .then(-1)
                            .otherwise(1)
                        )
                        .otherwise(0)
                    ),
                    False
                ),
                # non-sub plays last
                (pl.col('sub_index').lt(0), False),
                # sub out first
                (pl.col('sub_index').mod(2), True),
                ('team_id', False)
            )
        )
        .select(
            'game_id',
            'sequence_id',
            pl.col('home_id').fill_null(-1),
            pl.col('team_id').fill_null(-1),
            pl.col('player_id').fill_null(-1),
            pl.col('sub_index'),
        )
    )

    if isinstance(plays, pl.LazyFrame):
        plays_df = plays_frame.collect()
    else:
        plays_df = plays_frame

    game_id = plays_df.get_column('game_id').to_numpy()
    home_id = plays_df.get_column('home_id').to_numpy()
    team_id = plays_df.get_column('team_id').to_numpy()
    player_id = plays_df.get_column('player_id').to_numpy()
    sub_index = plays_df.get_column('sub_index').to_numpy()

    # allocate
    dummy_in_play = np.full(5, -1)
    home_in_play = np.full((game_id.shape[0], dummy_in_play.shape[0]), -1)
    away_in_play = np.full((game_id.shape[0], dummy_in_play.shape[0]), -1)

    _fill_in_play(
        game_id,
        home_id,
        team_id,
        player_id,
        sub_index,
        dummy_in_play,
        home_in_play,
        away_in_play,
    )

    return (
        plays
        .join(
            plays_frame.select(
                'game_id',
                'sequence_id',
                pl.Series(home_in_play).alias('home_in_play'),
                pl.Series(away_in_play).alias('away_in_play'),
            ),
            on=['game_id', 'sequence_id']
        )
    )
