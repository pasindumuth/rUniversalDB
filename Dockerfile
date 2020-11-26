FROM runiversal:latest
WORKDIR /home
# If we delete files, make sure to run rm -rf to get these files
# out from the previous version of runiversal we bring in. The COPY
# command doesn't remove the for us.
# RUN rm -rf ./*
COPY ./ ./
RUN cargo build
