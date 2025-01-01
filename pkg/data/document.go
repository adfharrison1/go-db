package data

type Document map[string]interface{}
type Collection struct {
    Name      string
    Documents map[string]Document 
}

func NewCollection(name string) *Collection {
    return &Collection{
        Name:      name,
        Documents: make(map[string]Document),
    }
}
