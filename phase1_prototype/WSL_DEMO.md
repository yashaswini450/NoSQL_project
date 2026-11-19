# WSL presentation demo

On the prepared computer, open PowerShell and attach to the ready demo:

```powershell
wsl -d Ubuntu-24.04 --exec tmux attach -t nasa-demo
```

You can also double-click `Open-WSL-Demo.cmd` in this folder. It opens the same menu using the existing WSL runtime and PostgreSQL database.

The native WSL checkout is `~/nosql-demo`. From Ubuntu:

```bash
cd ~/nosql-demo/phase1_prototype
bash demo.sh
```

To recreate the prepared tmux session after a WSL restart, run `bash prepare-demo.sh` from that directory, then attach again.

Suggested presentation sequence:

1. Choose **1** for the guided tour: runtime readiness, completed MapReduce results, real SQL storage and a fresh SQL equivalence check.
2. Choose **6** to show the actual 23 Hadoop job IDs and native counters from the completed full-data run.
3. Choose **7** to start a fresh MapReduce run on the **complete official August file**, with batches of 500,000 raw lines. This takes several minutes. Explain the mapper, shuffle and reducers while it runs.
4. When it finishes, the separate reporting module reads the new results from PostgreSQL. Choose **9** to show that new report again.

Options 1–6 inspect the previously completed official July-plus-August runs. They do not execute a new ETL run. Option 7 executes MapReduce; option 8 executes MongoDB. The interface labels the selected input and engine explicitly. Use the same files and batch size for a fair comparison.

Useful direct commands:

```bash
bash demo.sh check
bash demo.sh tour
bash demo.sh compare
bash demo.sh sql
bash demo.sh jobs
bash demo.sh live-mapreduce --month aug --batch-size 500000
bash demo.sh live-mongodb --month aug --batch-size 500000
bash demo.sh latest
```

The startup script starts the private MongoDB and PostgreSQL services. It does not download files, modify raw data or reinstall software. If setting up a different computer, follow `README.md` first, then either execute new runs or restore the included historical SQL reports into an empty database.

Tmux keeps the prepared menu available if a terminal disconnects. Press **Ctrl+B**, then **D**, to detach; use the attach command again to return. There is also a `shell` window; press **Ctrl+B**, then **N**, to switch windows. If the session was closed, restart with `bash demo.sh` from the project folder.

For a fresh clone in a different location, the menu works from that location; the `~/nosql-demo` path and ready tmux session describe this prepared computer.
