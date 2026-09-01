FROM grafana/alloy:v1.19.2

COPY config.alloy /etc/alloy/config.alloy

EXPOSE 12345

CMD ["run", "--server.http.listen-addr=0.0.0.0:12345", "--storage.path=/var/lib/alloy/data", "/etc/alloy/config.alloy"]
