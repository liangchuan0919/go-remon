package store

const Nil = StoreError("store: nil")

type StoreError string

func (e StoreError) Error() string { return string(e) }
