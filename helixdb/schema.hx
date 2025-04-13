N::Users {
	Name: Text,
	Age: Integer,
	City: Text
}

N::Parents {
	Name: Text,
	Age: Integer,
	GrewUpIn: Text
}

E::UsersToParents {
	From: Users
	To: Parents
	Properties: {
	}

}

