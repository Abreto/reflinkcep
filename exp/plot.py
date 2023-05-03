# %%
# deps: pandas, openpyxl, matplotlib
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# %%

res = pd.read_excel('results-collection/total.xlsx')

# %%
import matplotlib
matplotlib.rcParams['font.family'] = ['Heiti TC']

# ax1 = res.plot.scatter(x='our_ms', y='flinkcep_ms', c='DarkBlue')
ax1 = res.plot.scatter(x='our_ms', y='flinkcep_ms', c='black', s=2, title='FlinkCEP查询求值系统与FlinkCEP官方实现效率比较')

lims = [
    np.min([ax1.get_xlim(), ax1.get_ylim()]),
    np.max([ax1.get_xlim(), ax1.get_ylim()]),
]
# lims = [
#     np.max([ax1.get_xlim()[0], ax1.get_ylim()[0]]),
#     np.min([ax1.get_xlim()[1], ax1.get_ylim()[1]]),
# ]
# ax1.plot(lims, lims, 'k--', alpha=0.75, zorder=0)
ax1.plot(lims, lims, '--', alpha=0.75, zorder=0, label='y=x')

ax1.set_xlabel('FlinkCEP查询求值系统用时 (ms)')
ax1.set_ylabel('FlinkCEP官方实现用时 (ms)')


# %%

ax2 = res.plot.scatter(x='our_ms', y='flinkcep_ms', c='div')

# %%
import matplotlib
import matplotlib.font_manager
print(matplotlib.matplotlib_fname())
matplotlib.font_manager._rebuild()

# %%
