package collection

type contentCollection struct {
	UUID             string   `json:"uuid"`
	Items            []item   `json:"items,omitempty"`
	PublishReference string   `json:"publishReference"`
	LastModified     string   `json:"lastModified"`
	Publication      []string `json:"publication,omitempty"`
}

type item struct {
	UUID string `json:"uuid,omitempty"`
}
