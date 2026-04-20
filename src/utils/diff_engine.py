"""Compute and render before/after DataFrame diffs for the Changes tab."""
import pandas as pd
import streamlit as st

_MAX_DIFF_ROWS_SHOWN = 50


def compute_diff(before: pd.DataFrame, after: pd.DataFrame) -> dict:
    """Return a structured diff between two DataFrames.

    Handles three kinds of changes:
      - Cell value changes (same row, same column, different value)
      - Rows removed (in before, absent in after by index)
      - Rows added (in after, absent in before by index)
    """
    before_idx = before.index
    after_idx = after.index
    removed_idx = before_idx.difference(after_idx)
    added_idx = after_idx.difference(before_idx)
    shared_idx = before_idx.intersection(after_idx)

    rows_removed = len(removed_idx)
    rows_added = len(added_idx)

    # Cell-level changes on rows present in both
    changed_rows_frame = pd.DataFrame()
    cells_changed = 0
    columns_affected: list[str] = []
    if len(shared_idx) > 0:
        shared_before = before.loc[shared_idx]
        shared_after = after.loc[shared_idx]
        shared_cols = shared_before.columns.intersection(shared_after.columns)
        if len(shared_cols) > 0:
            b = shared_before[shared_cols]
            a = shared_after[shared_cols]
            # Element-wise inequality accounting for NaN-vs-NaN being equal
            ne_mask = (b != a) & ~(b.isna() & a.isna())
            cells_changed = int(ne_mask.to_numpy().sum())
            columns_affected = [c for c in shared_cols if bool(ne_mask[c].any())]
            row_mask = ne_mask.any(axis=1)
            if row_mask.any():
                changed_rows_frame = _build_before_after_frame(
                    b.loc[row_mask], a.loc[row_mask], columns_affected
                )

    rows_changed = 0 if changed_rows_frame.empty else len(changed_rows_frame)

    return {
        'rows_changed': rows_changed,
        'rows_removed': rows_removed,
        'rows_added': rows_added,
        'cells_changed': cells_changed,
        'columns_affected': columns_affected,
        'changed_rows': changed_rows_frame,
        'removed_rows': before.loc[removed_idx] if rows_removed else pd.DataFrame(),
        'added_rows': after.loc[added_idx] if rows_added else pd.DataFrame(),
    }


def _build_before_after_frame(
    before: pd.DataFrame, after: pd.DataFrame, cols: list[str]
) -> pd.DataFrame:
    """Interleave before/after values into a flat display frame.

    Output columns: ['row', 'col1 (before)', 'col1 (after)', 'col2 (before)', ...]
    """
    out = pd.DataFrame(index=range(len(before)))
    out['row'] = before.index
    for col in cols:
        out[f'{col} (before)'] = before[col].to_numpy()
        out[f'{col} (after)'] = after[col].to_numpy()
    return out


def render_diff(diff: dict) -> None:
    """Render a diff dict as Streamlit components."""
    total_changes = diff['rows_changed'] + diff['rows_removed'] + diff['rows_added']
    if total_changes == 0:
        st.caption("_No row-level changes._ (Dtype cast or flag column may still have been applied.)")
        return

    col1, col2, col3, col4 = st.columns(4)
    col1.metric('Rows changed', diff['rows_changed'])
    col2.metric('Rows removed', diff['rows_removed'])
    col3.metric('Rows added', diff['rows_added'])
    col4.metric('Cells changed', diff['cells_changed'])

    if diff['columns_affected']:
        st.caption(f"Affected columns: {', '.join(f'`{c}`' for c in diff['columns_affected'])}")

    changed = diff['changed_rows']
    if not changed.empty:
        st.write('**Changed rows** (before → after)')
        st.dataframe(
            changed.head(_MAX_DIFF_ROWS_SHOWN),
            use_container_width=True,
            hide_index=True,
        )
        if len(changed) > _MAX_DIFF_ROWS_SHOWN:
            st.caption(f"Showing first {_MAX_DIFF_ROWS_SHOWN} of {len(changed)} changed rows.")

    if diff['rows_removed']:
        with st.expander(f"Removed rows ({diff['rows_removed']})"):
            st.dataframe(diff['removed_rows'].head(_MAX_DIFF_ROWS_SHOWN), use_container_width=True)

    if diff['rows_added']:
        with st.expander(f"Added rows ({diff['rows_added']})"):
            st.dataframe(diff['added_rows'].head(_MAX_DIFF_ROWS_SHOWN), use_container_width=True)
