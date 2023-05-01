# %%
# deps: pandas, openpyxl, matplotlib
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# %%

res = pd.read_excel('results-collection/total.xlsx')

# %%

# ax1 = res.plot.scatter(x='our_ms', y='flinkcep_ms', c='DarkBlue')
ax1 = res.plot.scatter(x='our_ms', y='flinkcep_ms', c='black', s=2)

# lims = [
#     np.min([ax1.get_xlim(), ax1.get_ylim()]),
#     np.max([ax1.get_xlim(), ax1.get_ylim()]),
# ]
lims = [
    np.max([ax1.get_xlim()[0], ax1.get_ylim()[0]]),
    np.min([ax1.get_xlim()[1], ax1.get_ylim()[1]]),
]
# ax1.plot(lims, lims, 'k--', alpha=0.75, zorder=0)
ax1.plot(lims, lims, '--', alpha=0.75, zorder=0, label='y=x')


# %%

ax2 = res.plot.scatter(x='our_ms', y='flinkcep_ms', c='div')

# %%
