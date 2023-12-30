import customtkinter
from tkinter_helpers import btn_handler
import zope.interface
from observer.observer_pattern import IObserver
from oracle import run_it as sanim
from mashaghelsonati import get_tashkhis_ghatee_sonati as mash
from arzeshafzoodehsonati import run_it as arzesh
from soratmoamelat import run_it as soratmoamelat


@zope.interface.implementer(IObserver)
class App(customtkinter.CTk):
    def __init__(self, data_source=None):
        super().__init__()
        self.data_source = data_source
        self.geometry("800x600")
        self.title("CTk example")

        # add widgets to app
        self.button_1 = customtkinter.CTkButton(
            self, command=lambda: btn_th(self.update_btn1),
            text='صورت معاملات',
            font=customtkinter.CTkFont('Tahoma', size=15, weight="bold"))
        self.button_1.grid(row=0, column=0, padx=20, pady=10)

        self.button_2 = customtkinter.CTkButton(
            self, command=lambda: btn_th(self.update_btn2),
            text='ارزش افزوده',
            font=customtkinter.CTkFont('Tahoma', size=15, weight="bold"))
        self.button_2.grid(row=1, column=0, padx=20, pady=10)

        self.button_3 = customtkinter.CTkButton(
            self,  command=lambda: btn_th(self.update_btn3),
            text='مشاغل',
            font=customtkinter.CTkFont('Tahoma', size=15, weight="bold"))
        self.button_3.grid(row=2, column=0, padx=20, pady=10)

        self.button_4 = customtkinter.CTkButton(
            self, command=lambda: btn_th(self.update_btn4),
            text='سنیم',
            font=customtkinter.CTkFont('Tahoma', size=15, weight="bold"))
        self.button_4.grid(row=3, column=0, padx=20, pady=10)

        self.textbox_1 = customtkinter.CTkTextbox(self, width=500, height=100)
        self.textbox_1.grid(row=0, column=1, padx=(
            20, 0), pady=(20, 0), sticky="nsew")
        self.textbox_2 = customtkinter.CTkTextbox(self, width=500, height=100)
        self.textbox_2.grid(row=1, column=1, padx=(
            20, 0), pady=(20, 0), sticky="nsew")
        self.textbox_3 = customtkinter.CTkTextbox(self, width=500, height=100)
        self.textbox_3.grid(row=2, column=1, padx=(
            20, 0), pady=(20, 0), sticky="nsew")
        self.textbox_4 = customtkinter.CTkTextbox(self, width=500, height=100)
        self.textbox_4.grid(row=3, column=1, padx=(
            20, 0), pady=(20, 0), sticky="nsew")

        # Set default values
        self.textbox_1.insert("0.0", self.data_source.get_txt1_val())
        self.textbox_2.insert("0.0", self.data_source.get_txt2_val())
        self.textbox_3.insert("0.0", self.data_source.get_txt3_val())
        self.textbox_4.insert("0.0", self.data_source.get_txt4_val())

    def update(self, field, fieldname):
        # self.value = self.data_source.get_value()
        if fieldname == '1':
            field.insert("end", self.data_source.get_txt1_val())

        if fieldname == '2':
            field.insert("end", self.data_source.get_txt2_val())

        if fieldname == '3':
            field.insert("end", self.data_source.get_txt3_val())

        if fieldname == '4':
            field.insert("end", self.data_source.get_txt4_val())

    # add methods to app
    def update_btn1(self):
        self.textbox_1.delete('1.0', 'end')
        # btn_handler(
        #     soratmoamelat, 'soratmoamelat', field=self.data_source.set_txt1_val)
        soratmoamelat(field=self.data_source.set_txt1_val)

    def update_btn2(self):
        self.textbox_2.delete('1.0', 'end')
        arzesh(field=self.data_source.set_txt2_val)

    def update_btn3(self):
        self.textbox_3.delete('1.0', 'end')
        mash(field=self.data_source.set_txt3_val)

    def update_btn4(self):
        self.textbox_4.delete('1.0', 'end')
        sanim(field=self.data_source.set_txt4_val)


def btn_th(task):
    import threading
    t1 = threading.Thread(target=task)
    t1.start()
    x = 12


if __name__ == "__main__":
    from observer.observer_pattern import DataSource
    data_source = DataSource()
    app = App(data_source)
    data_source.add_observer(app)

    app.mainloop()
