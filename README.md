# rockset_sqlalchemy
This repo implements Python's dbapi spec and provides SQLAlchemy support on top of Rockset.

# Development
Iterating on this library is very simple.

All you need to do is run `sudo python3 setup.py develop` and hack away.

You can use the example script `example.py` to get started with development.

Make sure you provide a `ROCKSET_API_KEY` and `ROCKSET_API_SERVER` to the script, like so

```
ROCKSET_API_KEY=xxx ROCKSET_API_SERVER=https://api.rs2.usw2.rockset.com python3 example.py
```
