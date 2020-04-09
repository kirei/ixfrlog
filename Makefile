SOURCE=	ixfrlog.py watchlist.py

run:
	python3 ixfrlog.py

reformat:
	isort $(SOURCE)
	black $(SOURCE)

clean:
	rm -f *.tmp
