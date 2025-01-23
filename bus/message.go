package bus

//Message - interface that should be implemented by all structs representing messages
type Message interface {
	Route() string
	Exchange() string
}
