"""Generate bar charts of results."""
from typing import TYPE_CHECKING
import duckdb
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

if TYPE_CHECKING:
    import pandas as pd

with duckdb.connect(database="storage-benchmark.duckdb", read_only=True) as con:
    read_results: "pd.DataFrame" = con.execute(
        """
    select
        phase_name,
        format,
        geomean_duration_sec,
    from main.geomeans
    where type = 'read'
    """
    ).df()

    write_results: "pd.DataFrame" = con.execute(
        """
    select
        phase_name,
        format,
        geomean_duration_sec,
    from main.geomeans
    where type = 'write'
    """
    ).df()

    read_diff: "pd.DataFrame" = con.execute(
        """
    select
        phase_name,
        read_ratio,
    from main.read_results
    """
    ).df()

    write_diff: "pd.DataFrame" = con.execute(
        """
    select
        phase_name,
        write_ratio,
    from main.write_results
    """
    ).df()

    file_results: "pd.DataFrame" = con.execute(
        """
    select *
    from main.file_counts
        """
    ).df()

    file_diff: "pd.DataFrame" = con.execute(
        """
    select *
    from main.file_diffs
        """
    ).df()

    all_results: "pd.DataFrame" = con.execute(
        """
    select
        phase_name,
        format,
        sum(geomean_duration_sec) as geomean_duration_sec,
    from main.geomeans
    group by 1, 2
    """
    ).df()

read_ratios = read_diff.read_ratio.to_list()
write_ratios = write_diff.write_ratio.to_list()
file_ratios = file_diff.file_count_ratio.to_list()

# global grpah settings
LEGEND = {
    "orientation": "h",
    "yanchor": "bottom",
    "y": 1,
    "xanchor": "right",
    "x": 1,
    "bgcolor": "rgba(0,0,0,0)",
    "title": None,
}

TITLE = {
    "font": {"size": 56},
    "xanchor": "center",
    "yanchor": "top",
    "x": 0.5,
    "y": 0.95,
}

pio.templates["bdc"] = go.layout.Template(
    layout={
        "font": {"family": "Poppins", "size": 24},
        "hovermode": False,
        "legend": LEGEND,
        "title": TITLE,
        "autosize": True,
    }
)

pio.templates.default = "presentation+bdc"
pio.kaleido.scope.default_height = 1208
pio.kaleido.scope.default_width = 2544


# graph specific settings
COLORS = {
    "Delta Lake": "hsl(190,70%,20%)",
    "Apache Iceberg": "hsl(142,20%,60%)",
}

LABELS = {
    "phase_name": "Step",
    "geomean_duration_sec": "Geomean Duration (sec)<br /><sup>Lower is better</sup>",
    "format": "Storage Format",
}

ANNOTATION = {
    "xref": "x",
    "yref": "y",
    "font": {
        "color": "white",
        "size": 24,
    },
    "align": "center",
    "opacity": 0.9,
    "showarrow": False,
    "bgcolor": "darkgray",
    "borderwidth": 2,
    "borderpad": 4,
    "bordercolor": "gray",
}

read_duration = px.bar(
    read_results,
    x="phase_name",
    y="geomean_duration_sec",
    color="format",
    barmode="group",
    title="Read Performance",
    text_auto=True,
    labels=LABELS,
    color_discrete_map=COLORS,
)

read_duration.update_traces(textposition="outside")
read_duration.update_yaxes(automargin=True)
read_duration.update_layout(legend={"title": None})

for idx, ratio in zip(range(len(read_ratios)), read_ratios):
    read_duration.add_annotation(
        x=idx,
        y=50,
        text=f"{ratio}x longer",
        **ANNOTATION,
    )

read_duration.write_image("charts/read_duration.png")

write_duration = px.bar(
    write_results,
    x="phase_name",
    y="geomean_duration_sec",
    color="format",
    barmode="group",
    title="Write Performance",
    text_auto=True,
    labels=LABELS,
    color_discrete_map=COLORS,
)

write_duration.update_traces(textposition="outside")
write_duration.update_yaxes(automargin=True)
write_duration.update_layout(legend={"title": None})

for idx, ratio in zip(range(len(write_ratios)), write_ratios):
    write_duration.add_annotation(
        x=idx,
        y=2250,
        text=f"{ratio}x longer",
        **ANNOTATION,
    )

write_duration.write_image("charts/write_duration.png")

file_counts = px.bar(
    file_results,
    x="phase",
    y="total_files",
    color="format",
    barmode="group",
    text_auto=True,
    title="File Counts",
    labels={
        "phase": "Step",
        "total_files": "Total Files in Table<br /><sup>Lower is better</sup>",
        "format": "Storage Format",
    },
    color_discrete_map=COLORS,
)

file_counts.update_traces(textposition="outside")
file_counts.update_yaxes(automargin=True)
file_counts.update_layout(legend={"title": None})

for idx, ratio in zip(range(len(file_ratios)), file_ratios):
    file_counts.add_annotation(
        x=idx,
        y=60000,
        text=f"{ratio}x more files",
        **ANNOTATION,
    )

file_counts.write_image("charts/file_counts.png")

all_duration = px.bar(
    all_results,
    x="phase_name",
    y="geomean_duration_sec",
    color="format",
    barmode="group",
    title="Total Duration",
    text_auto=True,
    labels=LABELS,
    color_discrete_map=COLORS,
)

all_duration.update_traces(textposition="outside")
all_duration.update_yaxes(automargin=True)
all_duration.update_layout(legend={"title": None})
all_duration.write_image("charts/total_duration.png")
