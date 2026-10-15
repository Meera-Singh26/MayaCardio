# AI tool disclosure
* **Tool**: Claude (Anthropic) used as a coding assistant to write the pipeline, the test harness and these documents.
* **Where it helped**: architecture draft, implementation of collector/uploader/cloud service, fault-injection mock, tests.
* **What I verified independently** (fill in / re-run yourself before submitting): `python3 -m unittest tests.test_windowing`
  against an independent naive implementation; the end-to-end failure tests in `tests/` and `evidence/`; I can explain every module.
* **Claim I trust least**: durability on a real SD card under a real power cut (I only tested `kill -9`), and results-field naming
  because the brief does not define the JSON shape of result lines.
* **Not the judges' tools**: `tools/biosignal_mock.py` and `tools/ground_truth.py` are my own stand-ins.
