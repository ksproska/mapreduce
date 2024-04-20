send-to-lab:
	scp -r src/main/java/org/example/* adrian@172.26.87.135:/home/adrian/projects/pdzd/mapreduce/
	ssh adrian@172.26.87.135 docker cp /home/adrian/projects/pdzd/mapreduce/ master:/tmp/
