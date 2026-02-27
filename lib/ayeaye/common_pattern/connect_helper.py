class MultiConnectorNewDataset:
    """
    Multi-connectors are often used to produce a number of similar files. This helper class makes
    an additional method on a :class:`MultiConnector` connection to make it easy to create files
    using a templated engine_url.

    This is done using the `method_overlay` argument to the :class:`DataConnector` superclass. It
    can be used like this-

    # (as a class variable)
    output_file_template = "csv://{product_group}_{product_name}_parts_list.csv"
    components_doc = ayeaye.Connect(
        engine_url=[],
        method_overlay=(MultiConnectorNewDataset(template=output_file_template), "new_dataset"),
        access=ayeaye.AccessMode.WRITE,
    )
    ...
    # then use it like this ...
    components = self.components_doc.new_dataset(product_group="machinery", product_name="digger")

    # components is now a new CSV data connector, add something to it
    components.add({"name": "spring", "product_code": "ab123"})

    Also, see unittest :class:`TestConnectHelper`.
    """

    def __init__(self, template):
        """
        @param template: (str)
            template for an engine_url
        """
        self.template = template

    def local_resolve(self, *args, **kwargs):
        """
        Just using *args and **kwargs resolve template variables in `self.template`.

        This method is useful when the final engine_url needs to be known before decision is made
        to create a new dataset connection. The return from this method is a string which could
        still have template variables that can only be resolved by the `ayeaye.connector_resolver`
        global resolver.

        e.g.
        A new dataset should only be created if the existing file doesn't exist.

        output_file_template = "csv://{datasets_path}/{product_name}_parts.csv"
        new_datasets_builder = MultiConnectorNewDataset(template=output_file_template)
        components_doc = ayeaye.Connect(
            engine_url=output_file_template.replace("{dataset_name}", "*"),
            method_overlay=(new_datasets_builder, "new_dataset"),
            access=ayeaye.AccessMode.WRITE,
        )
        ...

        conditional_engine_url = new_datasets_builder.local_resolve(product_name="widget")

        with ayeaye.connect_resolve.connector_resolver.context(datasets_path="/data/parts/"):
            final_engine_url = ayeaye.connector_resolver.resolve(conditional_engine_url)

        if final_engine_url not in [d.engine_url for d in components_doc.data]:
            # Dataset not already on disk
            components = components_doc.new_dataset(product_name="widget")
            components.add({"name": "hello", "product_code": "new dataset"})


        @return: (str)
        """
        resolved_template = self.template
        for k, v in kwargs.items():
            template_field = "{" + k + "}"
            if template_field in self.template:
                # can't use .format as not all template fields are being replaced
                resolved_template = resolved_template.replace(template_field, v)

        return resolved_template

    def __call__(self, parent_connector, *args, **kwargs):
        """
        @param kwargs: dict with strings for both key and value
            These will be substituted into `self.template`
        """
        resolved_template = self.local_resolve(*args, **kwargs)
        new_dataset_connection = parent_connector.add_engine_url(resolved_template)
        return new_dataset_connection
